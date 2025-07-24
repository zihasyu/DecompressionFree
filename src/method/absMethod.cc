#include "../../include/absmethod.h"

AbsMethod::AbsMethod()
{
    mdCtx = EVP_MD_CTX_new();
    hashBuf = (uint8_t *)malloc(CHUNK_HASH_SIZE * sizeof(uint8_t));
    DecodeBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * 2);
    CombinedBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * 2);
}

AbsMethod::~AbsMethod()
{
    free(hashBuf);
    free(DecodeBuffer);
    free(CombinedBuffer);
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
void AbsMethod::SetTime(std::chrono::time_point<std::chrono::high_resolution_clock> atime, std::chrono::time_point<std::chrono::high_resolution_clock> btime, std::chrono::duration<double> &time)
{
    time += btime - atime;
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
uint8_t *AbsMethod::xd3_decode(const uint8_t *in, size_t in_size, const uint8_t *ref, size_t ref_size, size_t *res_size) // 更改函数
{
    const auto max_buffer_size = CONTAINER_MAX_SIZE * 2;
    size_t sz;
    auto ret = xd3_decode_memory(in, in_size, ref, ref_size, DecodeBuffer, &sz, max_buffer_size, 0);
    if (ret != 0)
    {
        cout << "decode error" << endl;
        cout << "ret code is " << ret << endl;
        const char *errMsg = xd3_strerror(ret);
        if (errMsg != nullptr)
        {
            printf("%s\n", errMsg);
        }
        else
        {
            printf("Unknown error\n");
        }
    }
    uint8_t *res;
    res = (uint8_t *)malloc(sz);
    *res_size = sz;
    memcpy(res, DecodeBuffer, sz);
    return res;
}

Chunk_t AbsMethod::xd3_recursive_restore_BL(uint64_t BasechunkId)
{
    std::vector<Chunk_t> chunkChain;
    Chunk_t basechunk;
    size_t basechunk_size = 0;

    // 收集delta链上的所有块
    chunkChain.push_back(dataWrite_->Get_Chunk_Info(BasechunkId));

    if (chunkChain.back().basechunkID < 0) // if only one layer
        return chunkChain.back();

    while (chunkChain.back().basechunkID > 0)
    {
        chunkChain.push_back(dataWrite_->Get_Chunk_Info(chunkChain.back().basechunkID));
    }

    basechunk.chunkPtr = (uint8_t *)malloc(CONTAINER_MAX_SIZE * sizeof(uint8_t));
    memcpy(basechunk.chunkPtr, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    basechunk.chunkSize = chunkChain.back().chunkSize;
    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr); // free base chunk memory

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        uint8_t *basechunk_ptr = xd3_decode(chunkChain[i].chunkPtr, chunkChain[i].saveSize,
                                            basechunk.chunkPtr, basechunk.chunkSize, &basechunk_size);
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        memcpy(basechunk.chunkPtr, basechunk_ptr, basechunk_size);
        basechunk.chunkSize = chunkChain[i].chunkSize; // update size
        free(basechunk_ptr);
        if (chunkChain[i].chunkSize != basechunk_size)
        {
            cout << "xd3 recursive restore error, chunk size mismatch" << endl;
            cout << "chunkid: " << chunkChain[i].chunkID << "chunkChain[i].chunkSize: " << chunkChain[i].chunkSize << ", basechunk_size: " << basechunk_size << endl;
        }
        basechunk_size = 0;
    }
    return basechunk;
}

Chunk_t AbsMethod::xd3_recursive_restore_BL_time(uint64_t BasechunkId)
{
    SetTime(startMiDelta);
    std::vector<Chunk_t> chunkChain;
    Chunk_t basechunk;
    size_t basechunk_size = 0;
    chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(BasechunkId));
    // if only one layer
    if (chunkChain.back().basechunkID < 0)
    {
        SetTime(startIO);
        chunkChain.back() = dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        // memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
        // basechunk.loadFromDisk = false;
        // basechunk.chunkSize = chunkChain.back().chunkSize;
        // basechunk.chunkPtr = CombinedBuffer;
        // return basechunk;
        return chunkChain.back();
    }

    // collect all delta chain blocks
    while (chunkChain.back().basechunkID > 0)
        chunkChain.push_back(dataWrite_->Get_Chunk_MetaInfo(chunkChain.back().basechunkID));
    // push the last chunk
    SetTime(startIO);
    chunkChain.back() = dataWrite_->Get_Chunk_Info(chunkChain.back().chunkID);
    SetTime(endIO);
    SetTime(startIO, endIO, IOTime);

    memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    basechunk.loadFromDisk = false;
    basechunk.chunkSize = chunkChain.back().chunkSize;
    basechunk.chunkPtr = CombinedBuffer;
    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr); // free base chunk memory

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        SetTime(startIO);
        chunkChain[i] = dataWrite_->Get_Chunk_Info(chunkChain[i].chunkID);
        SetTime(endIO);
        SetTime(startIO, endIO, IOTime);

        SetTime(startDecode);
        uint8_t *basechunk_ptr = xd3_decode(chunkChain[i].chunkPtr, chunkChain[i].saveSize,
                                            basechunk.chunkPtr, basechunk.chunkSize, &basechunk_size);
        SetTime(endDecode);
        DecodeTime += endDecode - startDecode;
        if (chunkChain[i].chunkSize != basechunk_size)
        {
            cout << "xd3 recursive restore error, chunk size mismatch" << endl;
            cout << "id " << chunkChain[i].chunkID << " chunkChain[i].chunkSize : " << chunkChain[i].chunkSize << "chunkChain[i].saveSize: " << chunkChain[i].saveSize
                 << " basechunksize " << basechunk.chunkSize << " restore basechunk_size : " << basechunk_size << endl;
            basechunk.chunkSize = 0;
            return basechunk;
        }
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        memcpy(CombinedBuffer, basechunk_ptr, basechunk_size);
        basechunk.chunkSize = chunkChain[i].chunkSize; // update size
        free(basechunk_ptr);

        basechunk_size = 0;
    }

    SetTime(endMiDelta);
    MiDeltaTime += endMiDelta - startMiDelta;
    return basechunk;
}

Chunk_t AbsMethod::xd3_recursive_restore_DF(uint64_t BasechunkId)
{
    std::vector<Chunk_t> chunkChain;
    Chunk_t result;
    size_t totalBufferSize = 0;

    chunkChain.push_back(dataWrite_->Get_Chunk_Info(BasechunkId));
    if (chunkChain.back().basechunkID < 0)
        return chunkChain.back();

    while (chunkChain.back().basechunkID > 0)
    {
        chunkChain.push_back(dataWrite_->Get_Chunk_Info(chunkChain.back().basechunkID));
    }

    memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    size_t currentSize = chunkChain.back().chunkSize;

    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr);

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        memcpy(CombinedBuffer + currentSize, chunkChain[i].chunkPtr, chunkChain[i].saveSize);
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        currentSize += chunkChain[i].saveSize;
    }
    result.chunkPtr = (uint8_t *)malloc(currentSize * sizeof(uint8_t));
    result.chunkSize = currentSize;
    memcpy(result.chunkPtr, CombinedBuffer, currentSize);
    if (chunkChain.back().chunkID == 422)
    {
        cout << "basechunkid " << chunkChain.back().chunkID << " currentSize " << currentSize << endl;
        cout << "As characters: ";
        for (size_t i = 0; i < currentSize; ++i)
        {
            // 对于换行符直接输出，对于空字符也不停止输出
            if (result.chunkPtr[i] == '\n')
                cout << endl;
            // 对于可打印字符直接输出，对于不可打印字符(包括\0)输出一个点
            else if (isprint(result.chunkPtr[i]))
                cout << result.chunkPtr[i];
            else
                cout << ".";
        }
        cout << endl; // 最后添加一个换行符，确保后续输出正常
    }
    return result;
}

size_t AbsMethod::VLVCompute(const uint8_t *xd3Data, size_t &offset)
{
    size_t dataSize = 0;
    do
    {
        dataSize = dataSize * 128 + (xd3Data[offset++] & 0x7F);
    } while (xd3Data[offset] >= 128);
    return dataSize;
}

int AbsMethod::xd3FindPatch(const uint8_t *xd3Data, size_t dataSize, size_t &offset)
{
    // VCDIFF 魔数检查 ("VCD" 序列)
    if (dataSize < 6)
    {
        return false;
    }
    offset = 6;
    // while (xd3Data[offset++] != '\0')
    // ;
    for (int i = 0; i < 4; i++)
        VLVCompute(xd3Data, offset); // 跳过4个VCDIFF头部字段
    offset++;                        // indicator
    size_t DataLength = VLVCompute(xd3Data, offset);
    size_t Instruction = VLVCompute(xd3Data, offset);
    size_t Address = VLVCompute(xd3Data, offset);
    cout << "ADDlength: " << DataLength << endl;
    cout << "Instruction: " << Instruction << endl;
    cout << "Address: " << Address << endl;
    return DataLength; // 返回处理后的偏移量
}

Chunk_t AbsMethod::xd3_recursive_restore_DF_pool(uint64_t BasechunkId)
{
    std::vector<Chunk_t> chunkChain;
    Chunk_t result;
    size_t totalBufferSize = 0;

    chunkChain.push_back(dataWrite_->Get_Chunk_Info(BasechunkId));
    if (chunkChain.back().basechunkID < 0)
        return chunkChain.back();

    while (chunkChain.back().basechunkID > 0)
    {
        chunkChain.push_back(dataWrite_->Get_Chunk_Info(chunkChain.back().basechunkID));
    }

    memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    size_t currentSize = chunkChain.back().chunkSize;

    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr);

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        size_t patchOffset = 13;
        size_t ADDlength = xd3FindPatch(chunkChain[i].chunkPtr, chunkChain[i].saveSize, patchOffset);
        // cout << "patchOffset: " << patchOffset << " ADDlength " << ADDlength << endl;
        // cout << "delta chunk content: " << chunkChain[i].chunkPtr + patchOffset << endl;
        cout << "delta chunk size: " << chunkChain[i].saveSize << endl;
        // printBinaryArray(chunkChain[i].chunkPtr, chunkChain[i].saveSize);
        memcpy(CombinedBuffer + currentSize, chunkChain[i].chunkPtr + patchOffset, ADDlength);
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        currentSize += ADDlength;
    }
    result.chunkPtr = CombinedBuffer;
    result.chunkSize = currentSize;
    result.loadFromDisk = false;
    // memcpy(result.chunkPtr, CombinedBuffer, currentSize);
    if (chunkChain.back().chunkID == 422)
    {
        cout << "basechunkid " << chunkChain.back().chunkID << " currentSize " << currentSize << endl;
        cout << "As characters: ";
        printBinaryArray(result.chunkPtr, currentSize);
    }
    return result;
}

Chunk_t AbsMethod::xd3_recursive_restore_DF_FindADD(uint64_t BasechunkId)
{
    std::vector<Chunk_t> chunkChain;
    Chunk_t result;
    size_t totalBufferSize = 0;

    chunkChain.push_back(dataWrite_->Get_Chunk_Info(BasechunkId));
    if (chunkChain.back().basechunkID < 0)
        return chunkChain.back();

    while (chunkChain.back().basechunkID > 0)
    {
        chunkChain.push_back(dataWrite_->Get_Chunk_Info(chunkChain.back().basechunkID));
    }

    memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    size_t currentSize = chunkChain.back().chunkSize;

    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr);

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        // cout << "delta chunk content: " << endl;
        // printBinaryArray(chunkChain[i].chunkPtr, chunkChain[i].saveSize);
        memcpy(CombinedBuffer + currentSize, chunkChain[i].chunkPtr, chunkChain[i].saveSize);
        auto patchOffset = 20;
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        currentSize += chunkChain[i].saveSize - patchOffset;
    }
    result.chunkPtr = CombinedBuffer;
    result.chunkSize = currentSize;
    result.loadFromDisk = false;
    // memcpy(result.chunkPtr, CombinedBuffer, currentSize);
    if (chunkChain.back().chunkID == 422)
    {
        cout << "basechunkid " << chunkChain.back().chunkID << " currentSize " << currentSize << endl;
        cout << "As characters: ";
        printBinaryArray(result.chunkPtr, currentSize);
    }
    return result;
}

Chunk_t AbsMethod::xd3_recursive_restore_DF(uint64_t BasechunkId, SuperFeatures superfeature, int *layer)
{
    std::vector<uint64_t> matchingIds = table.SF_Find_Mi(superfeature);
    std::vector<Chunk_t> chunkChain;
    Chunk_t result;
    int matchingSize = matchingIds.size();
    chunkChain.push_back(dataWrite_->Get_Chunk_Info(BasechunkId));
    *layer = matchingSize;

    if (matchingIds.size() == 1)
        if (chunkChain.back().basechunkID > 0)
        {
            size_t basechunk_size = 0;
            Chunk_t basechunk = dataWrite_->Get_Chunk_Info(chunkChain.back().basechunkID);
            result.chunkPtr = xd3_decode(chunkChain.back().chunkPtr, chunkChain.back().saveSize,
                                         basechunk.chunkPtr, basechunk.chunkSize, &basechunk_size);
            result.chunkSize = chunkChain.back().chunkSize;
            if (basechunk.loadFromDisk)
                free(basechunk.chunkPtr); // free base chunk memory
            if (chunkChain.back().loadFromDisk)
                free(chunkChain.back().chunkPtr); // free current chunk memory
            return result;
        }
        else
            return chunkChain.back();

    for (int i = matchingSize - 2; i >= 0; i--)
    {
        chunkChain.push_back(dataWrite_->Get_Chunk_Info(matchingIds[i]));
    }
    // Chunkchain.back() is delta chunk
    if (chunkChain.back().basechunkID > 0)
    {
        size_t basechunk_size = 0;
        Chunk_t basechunk = dataWrite_->Get_Chunk_Info(chunkChain.back().basechunkID);
        uint8_t *tmp = xd3_decode(chunkChain.back().chunkPtr, chunkChain.back().saveSize, // 不行，table的第一个不一定是delta chunk，而且她的base也不一定是。。
                                  basechunk.chunkPtr, basechunk.chunkSize, &basechunk_size);

        if (basechunk.loadFromDisk)
            free(basechunk.chunkPtr); // free base chunk memory
        if (chunkChain.back().loadFromDisk)
            free(chunkChain.back().chunkPtr); // free current chunk memory
        chunkChain.back().chunkPtr = tmp;
    }
    memcpy(CombinedBuffer, chunkChain.back().chunkPtr, chunkChain.back().chunkSize);
    size_t currentSize = chunkChain.back().chunkSize;

    if (chunkChain.back().loadFromDisk)
        free(chunkChain.back().chunkPtr);

    for (int i = chunkChain.size() - 2; i >= 0; i--)
    {
        memcpy(CombinedBuffer + currentSize, chunkChain[i].chunkPtr, chunkChain[i].saveSize);
        if (chunkChain[i].loadFromDisk)
            free(chunkChain[i].chunkPtr);
        currentSize += chunkChain[i].saveSize;
    }
    result.chunkPtr = (uint8_t *)malloc(currentSize * sizeof(uint8_t));
    result.chunkSize = currentSize;
    memcpy(result.chunkPtr, CombinedBuffer, currentSize);
    return result;
}
void AbsMethod::PrintChunkInfo(string inputDirpath, int chunkingMethod, int method, int fileNum, int64_t time, double ratio, double AcceptThreshold, bool IsFalseFilter)
{
    ofstream out;
    string fileName = "./chunkInfoLog.txt";
    if (!tool::FileExist(fileName))
    {
        out.open(fileName, ios::out);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./DFree -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
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
        out << "./DFree -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
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
        out << "./DFree -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
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
        out << "-----------------END-------------------------------" << endl;
    }
    else
    {
        out.open(fileName, ios::app);
        out << "-----------------INSTRUCTION----------------------" << endl;
        out << "./DFree -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
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
        out << "./DFree -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
        out << "-----------------Time old------------------------------" << endl;
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
        out << "./DFree -i " << CmdLine.dirName << " -c " << CmdLine.chunkingType << " -m " << CmdLine.compressionMethod << " -n " << CmdLine.backupNum << " -r " << CmdLine.ratio << " -a " << CmdLine.AcceptThreshold << " -b " << CmdLine.IsFalseFilter << " -t " << CmdLine.TurnOnNameHash << " -H " << CmdLine.MultiHeaderChunk << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
        out << "-----------------Time old------------------------------" << endl;
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
    cout << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
    cout << "IO Time: " << IOTime.count() << "s" << endl;
    cout << "Decode Time: " << DecodeTime.count() << "s" << endl;
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
        out << "./DFree -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
        out << "-----------------Time old------------------------------" << endl;
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
        out << "./DFree -i " << inputDirpath << " -c " << chunkingMethod << " -m " << method << " -n " << fileNum << " -r " << ratio << " -a " << AcceptThreshold << " -b " << IsFalseFilter << endl;
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
        out << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
        out << "IO Time: " << IOTime.count() << "s" << endl;
        out << "Decode Time: " << DecodeTime.count() << "s" << endl;
        out << "-----------------Time old------------------------------" << endl;
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
    cout << "MiDelta Time: " << MiDeltaTime.count() << "s" << endl;
    cout << "IO Time: " << IOTime.count() << "s" << endl;
    cout << "Decode Time: " << DecodeTime.count() << "s" << endl;
    cout << "-----------------Time old------------------------------" << endl;
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