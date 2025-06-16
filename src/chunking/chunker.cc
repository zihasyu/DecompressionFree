#include "../../include/chunker.h"

Chunker::Chunker(int chunkType_)
{

    FixedChunkSize = 8192;
    // specifiy chunk type
    chunkType = chunkType_;
    if (chunkType == MTAROdess)
    {
        chunkType = GEARCDC;
    }
    else if (chunkType == MTARPalantir)
    {
        chunkType = FASTCDC;
    }
    // init chunker
    name[100] = '\0';
    LongName[512] = '\0';
    ChunkerInit();
    // in different chunking method, the chunkBuffer is different
}
Chunker::~Chunker()
{
    free(readFileBuffer);
    free(chunkBuffer);

    if (chunkType == TAR_MultiHeader)
        free(headerBuffer);
}

void Chunker::LoadChunkFile(string path)
{
    if (inputFile.is_open())
    {
        inputFile.close();
    }
    // input_file_path_ = path; // 保存输入文件路径
    inputFile.open(path, ios_base::in | ios::binary);
    if (!inputFile.is_open())
    {
        tool::Logging(myName_.c_str(), "open file: %s error.\n",
                      path.c_str());
        exit(EXIT_FAILURE);
    }
    return;
}

void Chunker::ChunkerInit()
{
    switch (chunkType)
    {
    case FIXED_SIZE:
    {
        // fixed size chunking]
        readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
        chunkBuffer = (uint8_t *)malloc(FixedChunkSize);
        break;
    }
    case MTAR:

    case FASTCDC: // FastCDC chunking
    {
        readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
        chunkBuffer = (uint8_t *)malloc(MAX_CHUNK_SIZE);
        normalSize = CalNormalSize(minChunkSize, avgChunkSize, maxChunkSize);
        bits = (uint32_t)round(log2(static_cast<double>(avgChunkSize)));
        maskS = GenerateFastCDCMask(bits + 1);
        maskL = GenerateFastCDCMask(bits - 1);
        break;
    }
    case GEARCDC: // Gear chunking
    {
        readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
        chunkBuffer = (uint8_t *)malloc(MAX_CHUNK_SIZE);
        break;
    }
    case TAR:
    {

        readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
        chunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE); // 4MB
        normalSize = CalNormalSize(minChunkSize, avgChunkSize, maxChunkSize);
        bits = (uint32_t)round(log2(static_cast<double>(avgChunkSize)));
        maskS = GenerateFastCDCMask(bits + 1);
        maskL = GenerateFastCDCMask(bits - 1);
        break;
    }
    case TAR_MultiHeader:
    {
        readFileBuffer = (uint8_t *)malloc(READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
        headerBuffer = (uint8_t *)malloc(512 * (MULTI_HEADER_CHUNK + 1));
        chunkBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE); // 4MB
        // dataBuffer = (uint8_t *)malloc(CONTAINER_MAX_SIZE * 16); // 64MB
        normalSize = CalNormalSize(minChunkSize, avgChunkSize, maxChunkSize);
        bits = (uint32_t)round(log2(static_cast<double>(avgChunkSize)));
        maskS = GenerateFastCDCMask(bits + 1);
        maskL = GenerateFastCDCMask(bits - 1);
        break;
    }

    default:
        tool::Logging(myName_.c_str(), "chunk type error.\n");
        exit(EXIT_FAILURE);
        break;
    }
    return;
}

void Chunker::Chunking()
{
    bool end = false;
    uint64_t totalOffset = 0;

    while (!end)
    {
        memset((char *)readFileBuffer, 0, sizeof(uint8_t) * READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
        inputFile.read((char *)readFileBuffer, sizeof(uint8_t) * READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
        end = inputFile.eof();
        size_t len = inputFile.gcount();
        if (len == 0)
        {
            break;
        }
        localOffset = 0;
        while (((len - localOffset) >= CONTAINER_MAX_SIZE * MULTI_HEADER_CHUNK) || (end && (localOffset < len)))
        {
            // cout << " len is " << len << " localOffset is " << localOffset << endl;
            Chunk_t chunk;
            // compute cutPoint
            uint64_t cp = 0;
            switch (chunkType)
            {
            case FIXED_SIZE:
            {
                cp = avgChunkSize; // 8KB
                break;
            }
            case FASTCDC:
            {
                cp = CutPointFastCDC(readFileBuffer + localOffset, len - localOffset);
                break;
            }
            case GEARCDC:
            {
                cp = CutPointGear(readFileBuffer + localOffset, len - localOffset);
                break;
            }
            case TAR:
            {
                cp = CutPointTarFast(readFileBuffer + localOffset, len - localOffset);
                break;
            }
            case TAR_MultiHeader:
            {
                SetTime(startChunk);
                size_t cpOffset = CutPointTarHeader(readFileBuffer + localOffset, len - localOffset);
                localOffset += cpOffset;
                SetTime(endChunk);
                ChunkTime += (endChunk - startChunk);
                continue;
            }
            default:
                cout << "chunkType error" << endl;
                break;
            }
            chunk.chunkPtr = (uint8_t *)malloc(cp);
            memcpy(chunk.chunkPtr, readFileBuffer + localOffset, cp);
            chunk.chunkSize = cp;
            // chunk.chunkID = chunkID++;太早了
            if (cp == 0)
            {
                // cout << "cp is 0" << endl; // debug
                continue;
            }
            localOffset += cp;
            chunk.NameExist = true;
            if (!outputMQ_->Push(chunk))
            {
                tool::Logging(myName_.c_str(), "insert chunk to output MQ error.\n");
                exit(EXIT_FAILURE);
            }
        }
        totalOffset += localOffset;
        inputFile.seekg(totalOffset, ios_base::beg);
    }
    // cout << "chunking done." << endl;

    outputMQ_->done_ = true;
    tool::Logging(myName_.c_str(), "chunking done.\n");

    size_t element_size = sizeof(std::string);
    size_t num_elements = nameHashSet.size();
    size_t bucket_count = nameHashSet.bucket_count();
    size_t bucket_size = sizeof(void *); // 每个桶的指针大小

    size_t total_size = (element_size * num_elements) + (bucket_size * bucket_count);
    cout << "total_size is " << total_size << endl;
    return;
}

uint64_t Chunker::CutPointFastCDC(const uint8_t *src, const uint64_t len)
{
    uint64_t n;
    uint32_t fp = 0;
    uint64_t i;
    i = min(len, static_cast<uint64_t>(minChunkSize));
    n = min(normalSize, len);
    for (; i < n; i++)
    {
        fp = (fp >> 1) + GEAR[src[i]];
        if (!(fp & maskS))
        {
            return (i + 1);
        }
    }

    n = min(static_cast<uint64_t>(maxChunkSize), len);
    for (; i < n; i++)
    {
        fp = (fp >> 1) + GEAR[src[i]];
        if (!(fp & maskL))
        {
            return (i + 1);
        }
    }
    return i;
};
uint32_t Chunker::CutPointGear(const uint8_t *src, const uint64_t len)
{
    uint32_t fp = 0;
    uint64_t i = 0;
    for (; i < len; i++)
    {
        fp = (fp >> 1) + GEAR[src[i]];
        if (!(fp & MASK_GEAR))
        {
            return (i + 1);
        }
    }
    return i;
};
uint64_t Chunker::CutPointTarFast(const uint8_t *src, const uint64_t len)
{
    switch (Next_Chunk_Type)
    {
    case FILE_HEADER:
    {
        uint8_t data[12];
        std::memcpy(data, src + 124, 12);

        Next_Chunk_Size = 0;
        for (int i = 0; i < 11; i++)
        {
            Next_Chunk_Size = Next_Chunk_Size * 8 + data[i] - 48;
        }
        if (*(src + 156) == REGTYPE)
        {
            // edit: CONTAINER_MAX_SIZE
            if (Next_Chunk_Size <= BigChunkSize)
            {
                Next_Chunk_Type = FILE_CHUNK;
                FindName((char *)src);
                ExtractPath((char *)src);
            }
            else
            {
                Next_Chunk_Type = BIG_CHUNK;
                NameExist = true;
                Big_Chunk_Size = (Next_Chunk_Size + 511) / 512 * 512;
                Big_Chunk_Offset = 0;
                // Big_Chunk_Allowance = Next_Chunk_Size / CONTAINER_MAX_SIZE;
                // Big_Chunk_Last_Size = Next_Chunk_Size % CONTAINER_MAX_SIZE;
                // if (Big_Chunk_Last_Size == 0)
                // {
                //     Big_Chunk_Allowance--;
                //     Big_Chunk_Last_Size = CONTAINER_MAX_SIZE;
                // }
                // cout << "REGTYPE BigChunkSize is " << Big_Chunk_Size << endl;
            }
        }
        if (*(src + 156) == AREGTYPE)
        {
            Next_Chunk_Size = 0;
            for (int i = 0; i < 11; i++)
            {
                Next_Chunk_Size = Next_Chunk_Size * 8 + data[i];
            }
            // cout << "AREGTYPE ChunkSize is " << Next_Chunk_Size << endl;
            Next_Chunk_Size = 0;
            // edit: CONTAINER_MAX_SIZE
            if (Next_Chunk_Size <= BigChunkSize)
            {
                Next_Chunk_Type = FILE_CHUNK;
                FindName((char *)src);
                ExtractPath((char *)src);
            }
            else
            {
                Next_Chunk_Type = BIG_CHUNK;
                NameExist = true;
                Next_Chunk_Size = 0;
                for (int i = 0; i < 11; i++)
                {
                    Next_Chunk_Size = Next_Chunk_Size * 8 + data[i] - 48;
                    cout << data[i];
                }
                Big_Chunk_Size = (Next_Chunk_Size + 511) / 512 * 512;
                Big_Chunk_Offset = 0;
                // cout << "AREGTYPE BigChunkSize is " << Big_Chunk_Size << endl;
            }
        }
        if (*(src + 156) == 'x' || *(src + 156) == GNUTYPE_LONGNAME)
        {
            Next_Chunk_Type = FILE_CHUNK;
            IsLongNameChunk = true;
            FindName((char *)src);
            ExtractPath((char *)src);
        }
        /*use to debug*/
        // cout<<"Next_Chunk_Flag: " <<int(*(src + 156));
        // cout<<"Next_Chunk_Type: " <<Next_Chunk_Type;
        // cout<<"Next_Chunk_Size: "<<Next_Chunk_Size<<endl;
        // if(int(*(src + 156)) == 32||int(*(src + 156)) == 0){
        //     for(int i=0;i<512;i++)
        //     cout<<src[i]<<" ";
        //     cout<<endl;
        // }

        if (len >= 512)
            return 512;
        else
        {
            // printf("emmmm");
            return len;
        }
        break;
    }
    case FILE_CHUNK:
    {
        if (IsLongNameChunk)
        {
            FindLongName((char *)src);
            IsLongNameChunk = false;
        }
        uint64_t roundedUp = (Next_Chunk_Size + 511) / 512 * 512;
        Next_Chunk_Type = FILE_HEADER;
        Next_Chunk_Size = 512;
        if (roundedUp < len)
            return roundedUp;
        else
            return len;
        break;
    }
    case BIG_CHUNK:
    {
        if (Big_Chunk_Size - Big_Chunk_Offset > maxChunkSize)
        {
            // Big_Chunk_Allowance--;
            // cout << " BigChunkSize is " << Big_Chunk_Size << " BigChunkOffset is" << Big_Chunk_Offset << endl;
            uint64_t cp = CutPointFastCDC(src,
                                          Big_Chunk_Size - Big_Chunk_Offset);
            Big_Chunk_Offset += cp;
            // cout << "offset is " << Big_Chunk_Offset << " cp is " << cp << endl;
            return cp;
            // return CONTAINER_MAX_SIZE;
        }
        else
        {
            Next_Chunk_Type = FILE_HEADER;
            return Big_Chunk_Size - Big_Chunk_Offset;
        }
        break;
    }
    }
};
uint32_t Chunker::GenerateFastCDCMask(uint32_t bits)
{
    uint32_t tmp;
    tmp = (1 << CompareLimit(bits, 1, 31)) - 1;
    return tmp;
}
inline uint32_t Chunker::CompareLimit(uint32_t input, uint32_t lower, uint32_t upper)
{
    if (input <= lower)
    {
        return lower;
    }
    else if (input >= upper)
    {
        return upper;
    }
    else
    {
        return input;
    }
}
uint32_t Chunker::CalNormalSize(const uint32_t min, const uint32_t av, const uint32_t max)
{
    uint32_t off = min + DivCeil(min, 2);
    if (off > av)
    {
        off = av;
    }
    uint32_t diff = av - off;
    if (diff > max)
    {
        return max;
    }
    return diff;
}
inline uint32_t Chunker::DivCeil(uint32_t a, uint32_t b)
{
    uint32_t tmp = a / b;
    if (a % b == 0)
    {
        return tmp;
    }
    else
    {
        return (tmp + 1);
    }
}

uint64_t Chunker::CutPointTarHeader(const uint8_t *src, const uint64_t len)
// 调用CutPointTarFast，因为有NextChunkType的全局变量，所以断在哪里都没关系。但是为了减少recipe压力（一对segment可恢复），满足结尾时下一个type还是header即可。
{
    uint64_t blockTypeMask;
    uint64_t cpSum = 0;
    uint64_t loopTime = 1;
    if (Next_Chunk_Type == FILE_HEADER)
    {
        while ((HeaderCp < MULTI_HEADER_CHUNK * 512 || (localType == FILE_HEADER && Next_Chunk_Type == FILE_CHUNK)) && Next_Chunk_Type != BIG_CHUNK)
        // 当前是H下一个块也是H时，认为当前的H不指导切块，例如是目录，所以可以断。
        // 当前是D下一个块也是D时，应该是大块，也是可以断的。
        // 当前是D下一个块是H时，是正常的HD组合，也可以断。
        // 总结一下就是，当前为H，下一块为D时不可以断
        // 附加一条，下一个块是大块内容的时候也不在当前seg里切了
        {
            localType = Next_Chunk_Type;
            uint32_t cp = CutPointTarFast(src + cpSum, len - cpSum);

            if (localType == FILE_HEADER)
            {
                // ignore timestamp
                // uint8_t zero[12] = {0};
                // boundaries_.push_back({current_offset_ + cpSum, cp, 'H'});
                memcpy(headerBuffer + HeaderCp, src + cpSum, cp);
                HeaderCp += cp;
                // blockTypeMask = blockTypeMask;
            }
            else
            {
                if (cp == 0)
                {
                    // cout << "data cp is 0" << endl; // debug
                    continue;
                }
                // data chunking
                Chunk_t chunk;
                chunk.chunkPtr = (uint8_t *)malloc(cp);
                memcpy(chunk.chunkPtr, src + cpSum, cp);
                chunk.chunkSize = cp;
                chunk.NameExist = NameExist;
                chunk.name = hashNameToUint64(name);

                // 记录data边界
                // boundaries_.push_back({current_offset_ + cpSum, cp, 'D'});
                // input MQ
                if (!outputMQ_->Push(chunk))
                {
                    tool::Logging(myName_.c_str(), "insert chunk to output MQ error.\n");
                    exit(EXIT_FAILURE);
                }
                // mask
                blockTypeMask = blockTypeMask + loopTime;
                if (loopTime > UINT32_MAX)
                {
                    cout << "loopTime overflow is" << loopTime << endl;
                }
            }
            // local offset
            cpSum += cp;
            if (cpSum == len)
            {
                Next_Chunk_Type = FILE_HEADER;
                break; // 同时，这个backup结束了，可能要设计个flag
            }
            loopTime *= 2;
        }
        // input recipe MQ
        // if (!MaskoutputMQ_->Push(blockTypeMask))
        // {
        //     tool::Logging(myName_.c_str(), "insert chunk to output MQ error.\n");
        //     exit(EXIT_FAILURE);
        // }
        // multi header
        Chunk_t chunk;
        chunk.chunkPtr = (uint8_t *)malloc(HeaderCp);
        memcpy(chunk.chunkPtr, headerBuffer, HeaderCp);
        chunk.chunkSize = HeaderCp;
        chunk.HeaderFlag = true;
        chunk.NameExist = true;
        chunk.name = hashNameToUint64(path);
        // cout << "path is " << path << " namehash is " << chunk.name << endl;
        // reset
        HeaderCp = 0;
        // input chunk MQ
        if (!outputMQ_->Push(chunk))
        {
            tool::Logging(myName_.c_str(), "insert chunk to output MQ error.\n");
            exit(EXIT_FAILURE);
        }
    }
    else
    {
        // cout << " Next_Chunk_Type is " << Next_Chunk_Type << endl;
        //  不以header为开头只可能是bigchunk，这里想要的处理的bigchunk开头时
        // boundaries_.push_back({current_offset_, len, 'B'});
        while (Next_Chunk_Type != FILE_HEADER && cpSum < CONTAINER_MAX_SIZE - MAX_CHUNK_SIZE)
        // 当前是H下一个块也是H时，认为当前的H不指导切块，例如是目录，所以可以断。
        // 当前是D下一个块也是D时，应该是大块，也是可以断的。
        // 当前是D下一个块是H时，是正常的HD组合，也可以断。
        // 总结一下就是，当前为H，下一块为D时不可以断
        {
            localType = Next_Chunk_Type;
            uint32_t cp = CutPointTarFast(src + cpSum, len - cpSum);
            // cout << "big cdc size is " << cp << endl;
            if (localType == FILE_HEADER)
            {
                std::cout << "cut_bug";
                // memcpy(headerBuffer + HeaderCp, src + cpSum, cp);
                // HeaderCp += cp;
                // blockTypeMask = blockTypeMask;
            }
            else
            {
                Chunk_t chunk;
                chunk.chunkPtr = (uint8_t *)malloc(cp);
                memcpy(chunk.chunkPtr, src + cpSum, cp);
                chunk.chunkSize = cp;
                chunk.NameExist = true;
                // chunk.name = name;
                if (!outputMQ_->Push(chunk))
                {
                    tool::Logging(myName_.c_str(), "insert chunk to output MQ error.\n");
                    exit(EXIT_FAILURE);
                }
            }
            cpSum += cp;
            if (cpSum == len)
            {
                break; // 同时，这个backup结束了，可能要设计个flag
            }
        }
    }

    // cout << "HeaderCp is " << HeaderCp << endl;
    // cout << "DataCp is " << DataCp << endl;
    return cpSum;
}

void Chunker::MTar(vector<string> &readfileList, uint32_t backupNum)
{

    for (int i = 0; i < backupNum; i++)
    {
        auto startTmp = std::chrono::high_resolution_clock::now();
        string name;
        size_t pos = readfileList[i].find_last_of('/');
        if (pos != std::string::npos)
        {
            name = readfileList[i].substr(pos + 1);
        }
        else
        {
            name = readfileList[i];
        }
        string writePath = "./mTarFile/" + name + ".m";
        cout << "write path is " << writePath << endl;
        // stream set
        ifstream inFile(readfileList[i]);
        ofstream outFile(writePath);

        // 预留data_size空间
        uint64_t dataSize = 0;
        outFile.write((char *)&dataSize, sizeof(dataSize));

        // 记录data blocks起始位置
        std::streampos dataStart = outFile.tellp();
        // data chunk rewrite
        bool end = false;
        uint64_t totalOffset = 0;
        while (!end)
        {
            memset((char *)readFileBuffer, 0, sizeof(uint8_t) * READ_FILE_SIZE);
            inFile.read((char *)readFileBuffer, sizeof(uint8_t) * READ_FILE_SIZE);
            end = inFile.eof();
            size_t len = inFile.gcount();
            if (len == 0)
            {
                break;
            }
            localOffset = 0;
            while (((len - localOffset) >= CONTAINER_MAX_SIZE) || (end && (localOffset < len)))
            {
                // cout << " len is " << len << " localOffset is " << localOffset << endl;
                // compute cutPoint
                localType = Next_Chunk_Type;
                uint32_t cp = CutPointTarFast(readFileBuffer + localOffset, len - localOffset);
                if (cp == 0)
                {
                    continue;
                }
                if (localType != FILE_HEADER)
                {
                    outFile.write((char *)readFileBuffer + localOffset, cp);
                }

                localOffset += cp;
            }
            totalOffset += localOffset;
            inFile.seekg(totalOffset, ios_base::beg);
        }

        // 记录data blocks结束位置并计算大小
        std::streampos dataEnd = outFile.tellp();
        dataSize = dataEnd - dataStart;

        // reset
        localType = FILE_HEADER;
        Next_Chunk_Type = FILE_HEADER;
        ifstream inHeaderFile(readfileList[i]);
        // header chunk rewrite
        inHeaderFile.seekg(0, ios_base::beg);
        end = false;
        totalOffset = 0;
        while (!end)
        {
            memset((char *)readFileBuffer, 0, sizeof(uint8_t) * READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
            inHeaderFile.read((char *)readFileBuffer, sizeof(uint8_t) * READ_FILE_SIZE + MULTI_HEADER_CHUNK * CONTAINER_MAX_SIZE);
            end = inHeaderFile.eof();
            size_t len = inHeaderFile.gcount();
            if (len == 0)
            {
                break;
            }
            localOffset = 0;
            while (((len - localOffset) >= CONTAINER_MAX_SIZE) || (end && (localOffset < len)))
            {
                // cout << " len is " << len << " localOffset is " << localOffset << endl;
                // compute cutPoint
                localType = Next_Chunk_Type;
                uint32_t cp = CutPointTarFast(readFileBuffer + localOffset, len - localOffset);
                if (cp == 0)
                {
                    continue;
                }
                if (localType == FILE_HEADER)
                {
                    outFile.write((char *)readFileBuffer + localOffset, cp);
                }

                localOffset += cp;
            }
            totalOffset += localOffset;
            inHeaderFile.seekg(totalOffset, ios_base::beg);
        }

        // 回写data_size
        outFile.seekp(0);
        outFile.write((char *)&dataSize, sizeof(dataSize));
        cout << "offset is " << dataSize << endl;
        // reset
        localType = FILE_HEADER;
        Next_Chunk_Type = FILE_HEADER;
        inFile.close();
        inHeaderFile.close();
        outFile.close();
        // mtar overwrite the readfileList
        readfileList[i] = writePath;
        auto endTmp = std::chrono::high_resolution_clock::now();
        auto TimeTmp = std::chrono::duration_cast<std::chrono::duration<double>>(endTmp - startTmp).count();
        MTarTime.push_back(TimeTmp);
    }
    // reset
    chunkType = FASTCDC;
    localType = FILE_HEADER;
    Next_Chunk_Type = FILE_HEADER;
    return;
}

// 查找文件名是否已经存在于哈希表中
bool Chunker::FindName(const char *src)
{
    // 提取文件名
    const char *end = src + 100;
    const char *relativePath = std::find(src, end, '/');

    // 如果找到了'/'，则剔除其之前的内容
    if (relativePath != end)
    {
        relativePath++; // 跳过第一个'/'
    }
    else
    {
        relativePath = src; // 如果没有找到'/'，则使用原始src
    }
    std::copy(relativePath, src + 100, name);

    return 1;
}
bool Chunker::ExtractPath(const char *full)
{
    // 假定 full 为 "版本号/目录.../filename"
    // 并且成员变量 path 已经足够大（例如：char path[512];）

    // 找到第一个 '/'，版本号部分忽略
    const char *firstSlash = std::strchr(full, '/');
    if (!firstSlash)
    {
        // path[0] = '\0';
        return false;
    }
    // 从第一个 '/' 开始
    const char *remainder = firstSlash;

    // 找到最后一个 '/'（即分隔目录和文件名的 '/')
    const char *lastSlash = std::strrchr(remainder, '/');
    if (!lastSlash)
    {
        // 不可能发生，因为 firstSlash 一定存在
        std::strcpy(path, remainder);
        return true;
    }

    // 统计从 remainder 到 lastSlash 之间的 '/' 个数
    int slashCount = 0;
    const char *p = remainder;
    while (p < lastSlash)
    {
        if (*p == '/')
            ++slashCount;
        ++p;
    }

    if (slashCount == 1)
    {
        // 仅剩1个 '/'，舍弃最后一个 '/' 后的内容
        size_t len = lastSlash - remainder;
        std::strncpy(path, remainder, len);
        path[len] = '\0';
    }
    else if (slashCount > 2)
    {
        // 剩余 '/' 大于2，则舍弃倒数第二个 '/' 之后的内容
        const char *secondLast = nullptr;
        p = lastSlash - 1;
        while (p >= remainder)
        {
            if (*p == '/')
            {
                secondLast = p;
                break;
            }
            p--;
        }
        if (secondLast)
        {
            size_t len = secondLast - remainder;
            std::strncpy(path, remainder, len);
            path[len] = '\0';
        }
        else
        {
            std::strcpy(path, remainder);
        }
    }
    else
    {
        // 其他情况默认舍弃最后一个 '/' 后的内容
        size_t len = lastSlash - remainder;
        std::strncpy(path, remainder, len);
        path[len] = '\0';
    }

    return true;
}
bool Chunker::FindLongName(const char *src)
{
    // 提取文件名
    // 提取文件名
    const char *end = src + 512;
    const char *relativePath = std::find(src, end, '/');

    // 如果找到了'/'，则剔除其之前的内容
    if (relativePath != nullptr)
    {
        relativePath++; // 跳过第一个'/'
    }
    else
    {
        relativePath = src; // 如果没有找到'/'，则使用原始src
    }
    std::copy(relativePath, src + 512, LongName);
    // std::strncpy(LongName, src, 512);

    // 查找文件名是否存在于哈希表中
    // if (nameHashSet.find(std::string(LongName)) != nameHashSet.end())
    // {
    //     // cout << "do long exist name is " << name << endl;
    //     NameExist = 1;
    //     return 1; // 文件名已存在
    // }
    // else
    // {
    //     // cout << "no long exist name is " << name << endl;
    //     NameExist = 0;
    //     nameHashSet.insert(std::string(LongName));
    //     return 0; // 文件名不存在
    // }
    return 1;
}
const char *Chunker::FindNameBegin(const char *src)
{
    const char *end = src + 100;
    const char *relativePath = std::find(src, end, '/');

    // 如果找到了'/'，则剔除其之前的内容
    if (relativePath != end)
    {
        relativePath++; // 跳过第一个'/'
    }
    else
    {
        relativePath = src; // 如果没有找到'/'，则使用原始src
    }
    return relativePath;
}
const char *Chunker::FindLongNameBegin(const char *src)
{
    const char *end = src + 512;
    const char *relativePath = std::find(src, end, '/');

    // 如果找到了'/'，则剔除其之前的内容
    if (relativePath != end)
    {
        relativePath++; // 跳过第一个'/'
    }
    else
    {
        relativePath = src; // 如果没有找到'/'，则使用原始src
    }
    return relativePath;
}

uint16_t Chunker::hashNameToUint16(const char *name)
{
    uint16_t hash = 0;
    for (int i = 0; i < 101 && name[i] != '\0'; ++i)
    {
        hash = hash * 31 + name[i];
    }
    return hash;
}

uint64_t Chunker::hashNameToUint64(const char *name)
{
    uint64_t hash = 0;
    for (int i = 0; i < 101 && name[i] != '\0'; ++i)
    {
        hash = hash * prime + name[i];
    }
    return hash;
}

// void Chunker::WriteBoundariesToFile()
// {
//     // 获取文件名
//     std::string filename = input_file_path_.substr(input_file_path_.find_last_of("/\\") + 1);
//     std::string output_path = filename + ".boundaries";

//     std::ofstream out_file(output_path);
//     if (!out_file)
//     {
//         tool::Logging(myName_.c_str(), "Failed to open output file: %s\n", output_path.c_str());
//         return;
//     }

//     for (const auto &[offset, size, type] : boundaries_)
//     {
//         out_file << type << " " << offset << " " << size << "\n";
//     }
//     out_file.close();
// }

// void Chunker::SetHeaderChunkSize(uint64_t size)
// {
//     MultiHeaderSize = size;
//     return;
// }