#include "../../include/absmethod.h"

AbsMethod::AbsMethod()
{
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
}

AbsMethod::~AbsMethod()
{
    free(hashBuf);
}

void AbsMethod::SetFilename(string name)
{
    filename.assign(name);
    return;
}
void AbsMethod::SetTime(std::chrono::time_point<std::chrono::high_resolution_clock> &atime)
{
    atime = std::chrono::high_resolution_clock::now();
}
bool AbsMethod::compareNat(const std::string &a, const std::string &b)
{
    if (a.empty())
        return true;
    if (b.empty())
        return false;
    if (std::isdigit(a[0]) && !std::isdigit(b[0]))
        return true;
    if (!std::isdigit(a[0]) && std::isdigit(b[0]))
        return false;
    if (!std::isdigit(a[0]) && !std::isdigit(b[0]))
    {
        if (std::toupper(a[0]) == std::toupper(b[0]))
            return compareNat(a.substr(1), b.substr(1));
        return (std::toupper(a[0]) < std::toupper(b[0]));
    }

    // Both strings begin with digit --> parse both numbers
    std::istringstream issa(a);
    std::istringstream issb(b);
    int ia, ib;
    issa >> ia;
    issb >> ib;
    if (ia != ib)
        return ia < ib;

    // Numbers are the same --> remove numbers and recurse
    std::string anew, bnew;
    std::getline(issa, anew);
    std::getline(issb, bnew);
    return (compareNat(anew, bnew));
}

void AbsMethod::GenerateHash(EVP_MD_CTX *mdCtx, uint8_t *dataBuffer, const int dataSize, uint8_t *hash)
{
    int expectedHashSize = 0;

    if (!EVP_DigestInit_ex(mdCtx, EVP_sha256(), NULL))
    {
        fprintf(stderr, "CryptoTool: Hash init error.\n");
        exit(EXIT_FAILURE);
    }
    expectedHashSize = 32;

    if (!EVP_DigestUpdate(mdCtx, dataBuffer, dataSize))
    {
        fprintf(stderr, "CryptoTool: Hash error.\n");
        exit(EXIT_FAILURE);
    }
    uint32_t hashSize;
    if (!EVP_DigestFinal_ex(mdCtx, hash, &hashSize))
    {
        fprintf(stderr, "CryptoTool: Hash error.\n");
        exit(EXIT_FAILURE);
    }

    if (hashSize != expectedHashSize)
    {
        fprintf(stderr, "CryptoTool: Hash size error.\n");
        exit(EXIT_FAILURE);
    }

    EVP_MD_CTX_reset(mdCtx);
    return;
}

int AbsMethod::FP_Find(string fp)
{
    auto it = FPindex.find(fp);
    // cout << FPindex.size() << endl;
    if (it != FPindex.end())
    {
        // cout << "find fp" << endl;
        return it->second;
    }
    else
    {
        return -1;
    }
}

bool AbsMethod::FP_Insert(string fp, int chunkid)
{
    FPindex[fp] = chunkid;
    return true;
}

void AbsMethod::GetSF(unsigned char *ptr, EVP_MD_CTX *mdCtx, uint8_t *SF, int dataSize)
{
    std::mt19937 gen1, gen2; // 优化
    std::uniform_int_distribution<uint32_t> full_uint32_t;
    EVP_MD_CTX *mdCtx_ = mdCtx;
    int BLOCK_SIZE, WINDOW_SIZE;
    int SF_NUM, FEATURE_NUM;
    uint32_t *TRANSPOSE_M;
    uint32_t *TRANSPOSE_A;
    int *subchunkIndex;
    const uint32_t A = 37, MOD = 1000000007;
    uint64_t Apower = 1;
    uint32_t *feature;
    uint64_t *superfeature;
    gen1 = std::mt19937(922);
    gen2 = std::mt19937(314159);
    full_uint32_t = std::uniform_int_distribution<uint32_t>(std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max());

    BLOCK_SIZE = dataSize;
    WINDOW_SIZE = 48;
    SF_NUM = FINESSE_SF_NUM; // superfeature的个数
    FEATURE_NUM = 12;
    TRANSPOSE_M = new uint32_t[FEATURE_NUM];
    TRANSPOSE_A = new uint32_t[FEATURE_NUM];

    feature = new uint32_t[FEATURE_NUM];
    superfeature = new uint64_t[SF_NUM];
    subchunkIndex = new int[FEATURE_NUM + 1];
    subchunkIndex[0] = 0;
    for (int i = 0; i < FEATURE_NUM; ++i)
    {
        subchunkIndex[i + 1] = (BLOCK_SIZE * (i + 1)) / FEATURE_NUM;
    }
    for (int i = 0; i < FEATURE_NUM; ++i)
    {
        TRANSPOSE_M[i] = ((full_uint32_t(gen1) >> 1) << 1) + 1;
        TRANSPOSE_A[i] = full_uint32_t(gen1);
    }
    for (int i = 0; i < WINDOW_SIZE - 1; ++i)
    {
        Apower *= A;
        Apower %= MOD;
    }
    for (int i = 0; i < FEATURE_NUM; ++i)
        feature[i] = 0;
    for (int i = 0; i < SF_NUM; ++i)
        superfeature[i] = 0; // 初始化

    for (int i = 0; i < FEATURE_NUM; ++i)
    {
        int64_t fp = 0;

        for (int j = subchunkIndex[i]; j < subchunkIndex[i] + WINDOW_SIZE; ++j)
        {
            fp *= A;
            fp += (unsigned char)ptr[j];
            fp %= MOD;
        }

        for (int j = subchunkIndex[i]; j < subchunkIndex[i + 1] - WINDOW_SIZE + 1; ++j)
        {
            if (fp > feature[i])
                feature[i] = fp;

            fp -= (ptr[j] * Apower) % MOD;
            while (fp < 0)
                fp += MOD;
            if (j != subchunkIndex[i + 1] - WINDOW_SIZE)
            {
                fp *= A;
                fp += ptr[j + WINDOW_SIZE];
                fp %= MOD;
            }
        }
    }

    for (int i = 0; i < FEATURE_NUM / SF_NUM; ++i)
    {
        std::sort(feature + i * SF_NUM, feature + (i + 1) * SF_NUM);
    }
    int offset = 0;
    for (int i = 0; i < SF_NUM; ++i)
    {
        uint64_t temp[FEATURE_NUM / SF_NUM];
        for (int j = 0; j < FEATURE_NUM / SF_NUM; ++j)
        {
            temp[j] = feature[j * SF_NUM + i];
        }
        uint8_t *temp3;

        temp3 = (uint8_t *)malloc(FEATURE_NUM / SF_NUM * sizeof(uint64_t));

        memcpy(temp3, temp, FEATURE_NUM / SF_NUM * sizeof(uint64_t));

        uint8_t *temp2;
        temp2 = (uint8_t *)malloc(CHUNK_HASH_SIZE);
        this->GenerateHash(mdCtx_, temp3, sizeof(uint64_t) * FEATURE_NUM / SF_NUM, temp2);
        memcpy(SF + offset, temp2, CHUNK_HASH_SIZE);
        offset = offset + CHUNK_HASH_SIZE;
        free(temp2);
        free(temp3);
    }

    delete[] TRANSPOSE_M;
    delete[] TRANSPOSE_A;
    delete[] feature;
    delete[] superfeature;
    delete[] subchunkIndex;
    return;
}

int AbsMethod::SF_Find(const char *key, size_t keySize)
{
    string keyStr;
    for (int i = 0; i < FINESSE_SF_NUM; i++)
    {
        keyStr.assign(key + i * CHUNK_HASH_SIZE, CHUNK_HASH_SIZE);
        if (SFindex[i].find(keyStr) != SFindex[i].end())
        {
            // cout<<SFindex[i][keyStr].front()<<endl;
            return SFindex[i][keyStr].back();
        }
    }
    return -1;
}

bool AbsMethod::SF_Insert(const char *key, size_t keySize, int chunkid)
{
    string keyStr;
    for (int i = 0; i < FINESSE_SF_NUM; i++)
    {
        keyStr.assign(key + i * CHUNK_HASH_SIZE, CHUNK_HASH_SIZE);
        SFindex[i][keyStr].push_back(chunkid);
    }
    return true;
}

uint8_t *AbsMethod::xd3_encode(const uint8_t *targetChunkbuffer, size_t targetChunkbuffer_size, const uint8_t *baseChunkBuffer, size_t baseChunkBuffer_size, size_t *deltaChunkBuffer_size, uint8_t *tmpbuffer)
{
    size_t deltachunkSize;
    int ret = xd3_encode_memory(targetChunkbuffer, targetChunkbuffer_size, baseChunkBuffer, baseChunkBuffer_size, tmpbuffer, &deltachunkSize, CONTAINER_MAX_SIZE * 2, 0);
    if (ret != 0)
    {
        cout << "delta error" << endl;
        const char *errMsg = xd3_strerror(ret);
        cout << errMsg << endl;
    }
    uint8_t *deltaChunkBuffer;
    deltaChunkBuffer = (uint8_t *)malloc(deltachunkSize);
    *deltaChunkBuffer_size = deltachunkSize;
    memcpy(deltaChunkBuffer, tmpbuffer, deltachunkSize);
    return deltaChunkBuffer;
}

void AbsMethod::PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio, double AcceptThreshold, bool IsFalseFilter)
{
    ofstream out;
    string fileName = "./chunkInfoLog.txt";
    if (!tool::FileExist(fileName))
    {
        out.open(fileName, ios::out);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
        out << "-----------------CHUNK NUM-----------------------" << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;

        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    else
    {
        out.open(fileName, ios::app);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;

        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
        out << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
        out << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    out.close();
    return;
}
void AbsMethod::PrintChunkInfo(int64_t time, CommandLine_t CmdLine)
{
    ofstream out;
    string fileName = "./chunkInfoLog.txt";
    if (!tool::FileExist(fileName))
    {
        out.open(fileName, ios::out);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
        out << "-----------------CHUNK NUM-----------------------" << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;

        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
        out << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
        out << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    else
    {
        out.open(fileName, ios::app);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;

        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
        out << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
        out << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    out.close();
    return;
}

void AbsMethod::PrintChunkInfo(int64_t time, CommandLine_t CmdLine, double chunktime)
{
    ofstream out;
    string fileName = "./chunkInfoLog.txt";
    if (!tool::FileExist(fileName))
    {
        out.open(fileName, ios::out);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
        out << "-----------------CHUNK NUM-----------------------" << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;

        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "Chunk Time: " << chunktime << "s" << endl;
        out << "Dedup Time: " << DedupTime.count() << "s" << endl;
        out << "Locality Match Time: " << LocalityMatchTime.count() << "s" << endl;
        out << "Locality Delta Time: " << LocalityDeltaTime.count() << "s" << endl;
        out << "Feature Match Time: " << FeatureMatchTime.count() << "s" << endl;
        out << "Feature Delta Time: " << FeatureDeltaTime.count() << "s" << endl;
        out << "Lz4 Compression Time: " << lz4CompressionTime.count() << "s" << endl;
        out << "Delta Compression Time: " << deltaCompressionTime.count() << "s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
        out << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
        out << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "Feature reduct size: " << FeatureReduct << endl;
        out << "Locality reduct size: " << LocalityReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    else
    {
        out.open(fileName, ios::app);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
        out << "-----------------CHUNK NUM-----------------------" << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;

        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "Chunk Time: " << chunktime << "s" << endl;
        out << "Dedup Time: " << DedupTime.count() << "s" << endl;
        out << "Locality Match Time: " << LocalityMatchTime.count() << "s" << endl;
        out << "Locality Delta Time: " << LocalityDeltaTime.count() << "s" << endl;
        out << "Feature Match Time: " << FeatureMatchTime.count() << "s" << endl;
        out << "Feature Delta Time: " << FeatureDeltaTime.count() << "s" << endl;
        out << "Lz4 Compression Time: " << lz4CompressionTime.count() << "s" << endl;
        out << "Delta Compression Time: " << deltaCompressionTime.count() << "s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
        out << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
        out << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "Feature reduct size: " << FeatureReduct << endl;
        out << "Locality reduct size: " << LocalityReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    out.close();
    return;
}
void AbsMethod::StatsDelta(Chunk_t &tmpChunk)
{
    deltachunkOriSize += tmpChunk.chunkSize;
    deltachunkSize += tmpChunk.saveSize;
    deltachunkNum++;
    DeltaReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
    DCESum += tmpChunk.chunkSize / tmpChunk.saveSize;
}
void AbsMethod::StatsDeltaFeature(Chunk_t &tmpChunk)
{
    deltachunkOriSize += tmpChunk.chunkSize;
    deltachunkSize += tmpChunk.saveSize;
    deltachunkNum++;
    DeltaReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
    FeatureReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
    DCESum += tmpChunk.chunkSize / tmpChunk.saveSize;
}

void AbsMethod::StatsDeltaLocality(Chunk_t &tmpChunk)
{
    deltachunkOriSize += tmpChunk.chunkSize;
    deltachunkSize += tmpChunk.saveSize;
    deltachunkNum++;
    DeltaReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
    LocalityReduct += tmpChunk.chunkSize - tmpChunk.saveSize;
    DCESum += tmpChunk.chunkSize / tmpChunk.saveSize;
    LocalityDeltaTime += LocalityDeltaTmp;
}

void AbsMethod::Version_log(double time)
{
    cout << "Version: " << ads_Version << endl;
    cout << "-----------------CHUNK NUM-----------------------" << endl;
    cout << "logical chunk num: " << logicalchunkNum << endl;
    cout << "unique chunk num: " << uniquechunkNum << endl;
    cout << "base chunk num: " << basechunkNum << endl;
    cout << "delta chunk num: " << deltachunkNum << endl;
    cout << "-----------------CHUNK SIZE-----------------------" << endl;
    cout << "logicalchunkSize is " << logicalchunkSize << endl;
    cout << "uniquechunkSize is " << uniquechunkSize << endl;
    cout << "base chunk size: " << basechunkSize << endl;
    cout << "delta chunk size: " << deltachunkSize << endl;
    cout << "-----------------METRICS-------------------------" << endl;
    cout << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
    cout << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
    cout << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
    cout << "DCE: " << DCESum / (double)deltachunkNum << endl;
    cout << "-----------------Time------------------------------" << endl;
    // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
    cout << "Version time: " << time << "s" << endl;
    cout << "Throughput: " << (double)(logicalchunkSize - preLogicalchunkiSize) / time / 1024 / 1024 << "MiB/s" << endl;
    cout << "Reduce data speed: " << (double)(logicalchunkSize - preLogicalchunkiSize - uniquechunkSize + preuniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
    cout << "SF generation time: " << SFTime.count() - preSFTime.count() << "s" << endl;
    cout << "SF generation throughput: " << (double)(logicalchunkSize - preLogicalchunkiSize) / (SFTime.count() - preSFTime.count()) / 1024 / 1024 << "MiB/s" << endl;
    cout << "-----------------OverHead--------------------------" << endl;
    // out << "deltaCompressionTime: " << deltaCompressionTime.count() << "s" << endl;
    cout << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
    cout << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
    cout << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
    cout << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
    cout << "SF number: " << SFnum << endl;
    cout << "-----------------END-------------------------------" << endl;

    preLogicalchunkiSize = logicalchunkSize;
    preSFTime = SFTime;
}

void AbsMethod::PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio, double chunktime, double AcceptThreshold, bool IsFalseFilter)
{
    ofstream out;
    string fileName = "./chunkInfoLog.txt";
    if (!tool::FileExist(fileName))
    {
        out.open(fileName, ios::out);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
        out << "-----------------CHUNK NUM-----------------------" << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;

        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "Chunk Time: " << chunktime << "s" << endl;
        out << "Dedup Time: " << DedupTime.count() << "s" << endl;
        out << "Locality Match Time: " << LocalityMatchTime.count() << "s" << endl;
        out << "Locality Delta Time: " << LocalityDeltaTime.count() << "s" << endl;
        out << "Feature Match Time: " << FeatureMatchTime.count() << "s" << endl;
        out << "Feature Delta Time: " << FeatureDeltaTime.count() << "s" << endl;
        out << "Lz4 Compression Time: " << lz4CompressionTime.count() << "s" << endl;
        out << "Delta Compression Time: " << deltaCompressionTime.count() << "s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
        out << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
        out << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "Feature reduct size: " << FeatureReduct << endl;
        out << "Locality reduct size: " << LocalityReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    else
    {
        out.open(fileName, ios::app);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./BiSearch -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
        out << "-----------------CHUNK NUM-----------------------" << endl;
        out << "logical chunk num: " << logicalchunkNum << endl;
        out << "unique chunk num: " << uniquechunkNum << endl;
        out << "base chunk num: " << basechunkNum << endl;
        out << "delta chunk num: " << deltachunkNum << endl;
        out << "-----------------CHUNK SIZE-----------------------" << endl;
        out << "logical chunk size: " << logicalchunkSize << endl;
        out << "unique chunk size: " << uniquechunkSize << endl;
        out << "base chunk size: " << basechunkSize << endl;
        out << "delta chunk size: " << deltachunkSize << endl;
        out << "-----------------METRICS-------------------------" << endl;
        out << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
        out << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
        out << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
        out << "DCE: " << DCESum / (double)deltachunkNum << endl;
        out << "-----------------Time------------------------------" << endl;
        out << "total time: " << time << "s" << endl;
        out << "Throughput: " << (double)logicalchunkSize / time / 1024 / 1024 << "MiB/s" << endl;
        out << "Reduce data speed: " << (double)(logicalchunkSize - uniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
        out << "SF generation time: " << SFTime.count() << "s" << endl;
        out << "SF generation throughput: " << (double)logicalchunkSize / SFTime.count() / 1024 / 1024 << "MiB/s" << endl;
        out << "Chunk Time: " << chunktime << "s" << endl;
        out << "Dedup Time: " << DedupTime.count() << "s" << endl;
        out << "Locality Match Time: " << LocalityMatchTime.count() << "s" << endl;
        out << "Locality Delta Time: " << LocalityDeltaTime.count() << "s" << endl;
        out << "Feature Match Time: " << FeatureMatchTime.count() << "s" << endl;
        out << "Feature Delta Time: " << FeatureDeltaTime.count() << "s" << endl;
        out << "Lz4 Compression Time: " << lz4CompressionTime.count() << "s" << endl;
        out << "Delta Compression Time: " << deltaCompressionTime.count() << "s" << endl;
        out << "-----------------OverHead--------------------------" << endl;
        out << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
        out << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
        out << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
        out << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
        out << "SF number: " << SFnum << endl;
        out << "-----------------Reduct----------------------------" << endl;
        out << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
        out << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
        out << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
        out << "dedup reduct size : " << DedupReduct << endl;
        out << "delta reduct size : " << DeltaReduct << endl;
        out << "local reduct size : " << LocalReduct << endl;
        out << "Feature reduct size: " << FeatureReduct << endl;
        out << "Locality reduct size: " << LocalityReduct << endl;
        out << "-----------------Design 2 Motivation---------------" << endl;
        out << "case 1 OnlyFeature: " << OnlyFeature << endl;
        out << "case 2 SameCount:" << sameCount << endl;
        out << "case 3 OnlyMeta: " << OnlyMeta << endl;
        out << "case 4 DifferentCount: " << differentCount << endl;
        out << "-----------------END-------------------------------" << endl;
    }
    out.close();
    return;
}

void AbsMethod::Version_log(double time, double chunktime)
{
    cout << "Version: " << ads_Version << endl;
    cout << "-----------------CHUNK NUM-----------------------" << endl;
    cout << "logical chunk num: " << logicalchunkNum << endl;
    cout << "unique chunk num: " << uniquechunkNum << endl;
    cout << "base chunk num: " << basechunkNum << endl;
    cout << "delta chunk num: " << deltachunkNum << endl;
    cout << "-----------------CHUNK SIZE-----------------------" << endl;
    cout << "logicalchunkSize is " << logicalchunkSize << endl;
    cout << "uniquechunkSize is " << uniquechunkSize << endl;
    cout << "base chunk size: " << basechunkSize << endl;
    cout << "delta chunk size: " << deltachunkSize << endl;
    cout << "-----------------METRICS-------------------------" << endl;
    cout << "Overall Compression Ratio: " << (double)logicalchunkSize / (double)uniquechunkSize << endl;
    cout << "DCC: " << (double)deltachunkNum / (double)uniquechunkNum << endl;
    cout << "DCR: " << (double)deltachunkOriSize / (double)deltachunkSize << endl;
    cout << "DCE: " << DCESum / (double)deltachunkNum << endl;
    cout << "-----------------Time------------------------------" << endl;
    cout << "Version time: " << time << "s" << endl;
    cout << "Throughput: " << (double)(logicalchunkSize - preLogicalchunkiSize) / time / 1024 / 1024 << "MiB/s" << endl;
    cout << "Reduce data speed: " << (double)(logicalchunkSize - preLogicalchunkiSize - uniquechunkSize + preuniquechunkSize) / time / 1024 / 1024 << "MiB/s" << endl;
    cout << "SF generation time: " << SFTime.count() - preSFTime.count() << "s" << endl;
    cout << "SF generation throughput: " << (double)(logicalchunkSize - preLogicalchunkiSize) / (SFTime.count() - preSFTime.count()) / 1024 / 1024 << "MiB/s" << endl;
    cout << "Chunk Time: " << chunktime << "s" << endl;
    cout << "Dedup Time: " << DedupTime.count() << "s" << endl;
    cout << "Locality Match Time: " << LocalityMatchTime.count() << "s" << endl;
    cout << "Locality Delta Time: " << LocalityDeltaTime.count() << "s" << endl;
    cout << "Feature Match Time: " << FeatureMatchTime.count() << "s" << endl;
    cout << "Feature Delta Time: " << FeatureDeltaTime.count() << "s" << endl;
    cout << "Lz4 Compression Time: " << lz4CompressionTime.count() << "s" << endl;
    cout << "Delta Compression Time: " << deltaCompressionTime.count() << "s" << endl;
    cout << "-----------------OVERHEAD--------------------------" << endl;
    cout << "Index Overhead: " << (double)(uniquechunkNum * 112 + basechunkNum * 120) / 1024 / 1024 << "MiB" << endl;
    cout << "FP Overhead: " << (double)(uniquechunkNum * 80 + uniquechunkNum * 32) / 1024 / 1024 << "MiB" << endl;
    cout << "SF Overhead: " << (double)(basechunkNum * 120) / 1024 / 1024 << "MiB" << endl; //(3*(8+32)=120B)
    cout << "Recipe Overhead: " << (double)logicalchunkNum * 8 / 1024 / 1024 << "MiB" << endl;
    cout << "SF number: " << SFnum << endl;
    cout << "-----------------REDUCT----------------------------" << endl;
    cout << "Dedup ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct) << endl;
    cout << "Lossless ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct) << endl;
    cout << "Delta ratio : " << (double)logicalchunkSize / (double)(logicalchunkSize - DedupReduct - LocalReduct - DeltaReduct) << endl;
    cout << "dedup reduct size : " << DedupReduct << endl;
    cout << "delta reduct size : " << DeltaReduct << endl;
    cout << "local reduct size : " << LocalReduct << endl;
    cout << "Feature reduct size: " << FeatureReduct << endl;
    cout << "Locality reduct size: " << LocalityReduct << endl;
    cout << "-----------------Design 2 Motivation---------------" << endl;
    cout << "case 1 OnlyFeature: " << OnlyFeature << endl;
    cout << "case 2 SameCount:" << sameCount << endl;
    cout << "case 3 OnlyMeta: " << OnlyMeta << endl;
    cout << "case 4 DifferentCount: " << differentCount << endl;
    cout << "-----------------END-------------------------------" << endl;

    preLogicalchunkiSize = logicalchunkSize;
    preuniquechunkSize = uniquechunkSize;
    preSFTime = SFTime;
}