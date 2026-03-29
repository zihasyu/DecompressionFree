#include <iostream>
#include <string>
#include <csignal>
#include <sstream>
#include <chrono>
#include <filesystem>

#include "../../include/allmethod.h"

using namespace std;
namespace fs = std::filesystem;

void signalHandler(int signum)
{
    cout << "Interrupt signal (" << signum << ") received.\n";
    exit(signum);
}

void ResetDirectory(const std::string &path)
{
    std::error_code ec;
    fs::remove_all(path, ec);
    if (ec)
    {
        cerr << "Failed to clear directory " << path << ": " << ec.message() << endl;
        ec.clear();
    }
    fs::create_directories(path, ec);
    if (ec)
    {
        cerr << "Failed to create directory " << path << ": " << ec.message() << endl;
    }
}

int main(int argc, char **argv)
{
    signal(SIGINT, signalHandler);
    CommandLine_t CmdLine;
    // uint32_t chunkingType;
    // uint32_t compressionMethod;
    // uint32_t backupNum;
    // string dirName;
    string myName = "DFree";

    vector<string> readfileList;

    const char optString[] = "i:m:c:n:r:a:b:t:H:o:R:T:k:";
    // if (argc != sizeof(optString) && argc != sizeof(optString) - 2 && argc != sizeof(optString) - 4 && argc != sizeof(optString) - 6 && argc != sizeof(optString) - 8 && argc != sizeof(optString) - 10 && argc != sizeof(optString) - 12 && argc != sizeof(optString) - 14 && argc != sizeof(optString) - 16)
    // {
    //     cout << "argc is " << argc << endl;
    //     cout << "Usage: " << argv[0] << " -i <input file> -m <chunking method> -c <compression method> -n <process number> -r <Bisearch fault ratio> -a <False Filter Fixed parameters> -b <0 = fixed parameter> -t <0 = No meta-guided> -H <Multi Header num>" << endl;
    //     return 0;
    // }

    // Grab command-line instructions
    int option = 0;
    while ((option = getopt(argc, argv, optString)) != -1)
    {
        switch (option)
        {
        case 'i':
            CmdLine.dirName.assign(optarg);
            break;
        case 'c':
            CmdLine.chunkingType = atoi(optarg);
            break;
        case 'm':
            CmdLine.compressionMethod = atoi(optarg);
            break;
        case 'n':
            CmdLine.backupNum = atoi(optarg);
            break;
        case 'r':
            CmdLine.ratio = atoi(optarg);
            break;
        case 'a':
            CmdLine.AcceptThreshold = atoi(optarg);
            break;
        case 'b':
            CmdLine.IsFalseFilter = atoi(optarg);
            break;
        case 't':
            CmdLine.TurnOnNameHash = atoi(optarg);
            break;
        case 'H':
            CmdLine.MultiHeaderChunk = atoi(optarg);
            break;
        case 'o':
            CmdLine.offlineMethod = atoi(optarg);
            break;
        case 'R': // for restore
            CmdLine.enableRestore = atoi(optarg);
            break;
        case 'T':
            CmdLine.Threshold = atoi(optarg);
            break;
        case 'k':
            CmdLine.retentionBackups = atoi(optarg);
            break;
        default:
            break;
        }
    }
    if (CmdLine.dirName.empty() || CmdLine.chunkingType == -1 || CmdLine.compressionMethod == -1 || CmdLine.backupNum == -1)
    {
        cout << "Usage: " << argv[0] << " -i <input file> -c <chunking method> -m <compression method> -n <process number> [OPTIONS...]" << endl;
        cout << "Mandatory arguments:" << endl;
        cout << "  -i: Input directory" << endl;
        cout << "  -c: Chunking type (integer)" << endl;
        cout << "  -m: Compression method (integer)" << endl;
        cout << "  -n: Number of versions/backups to process" << endl;
        return 1;
    }

    AbsMethod *absMethodObj = nullptr, *OfflineAbsMethodObj = nullptr;
    Chunker *chunkerObj = new Chunker(CmdLine.chunkingType);

    MessageQueue<Chunk_t> *chunkerMQ = new MessageQueue<Chunk_t>(CHUNK_QUEUE_SIZE);

    switch (CmdLine.compressionMethod)
    {
    case DEDUP:
    {
        absMethodObj = new Dedup();
        break;
    }
    case NTRANSFORM:
    {
        absMethodObj = new NTransForm();
        break;
    }
    case FINESSE:
    {
        absMethodObj = new Finesse();
        break;
    }
    case ODESS:
    {
        absMethodObj = new Odess(CmdLine.offlineMethod);
        break;
    }
    case PALANTIR:
    {
        absMethodObj = new Palantir();
        break;
    }
    case BiSEARCH:
    {
        absMethodObj = new BiSearch(CmdLine.ratio); // Ratio is used to debug false filter, which is not used now.
        break;
    }
    case ODESS_MI_BL:
    {
        absMethodObj = new OdessMiBL();
        break;
    }
    case ODESS_MI_DF:
    {
        absMethodObj = new OdessMiDF();
        break;
    }
    case ODESS_MI_BL2:
    {
        absMethodObj = new OdessMiBL2();
        break;
    }
    case ODESS_MI_BL3:
    {
        absMethodObj = new OdessMiBL3();
        break;
    }
    case ODESS_ML_LOG2:
    {
        absMethodObj = new OdessMLLog2();
        break;
    }
    case ODESS_ML_LESS4:
    {
        absMethodObj = new OdessMLLess4();
        break;
    }
    case Tree_Cut:
    {
        absMethodObj = new TreeCut();
        break;
    }

    case Tree_Greedy:
    {
        absMethodObj = new TreeGreedy();
        break;
    }
    case All_Greedy:
    {
        absMethodObj = new AllGreedy(); // 14
        break;
    }
    case Tree_Cut_Layer:
    {
        absMethodObj = new TreeCutLayer(); // 15
        break;
    }
    case Tree_Cache:
    {
        absMethodObj = new TreeCache();
        break;
    }
    case Tree_Cache2:
    {
        absMethodObj = new TreeCache2();
        break;
    }
    case SUBTREE_REDUCTION:
    {
        absMethodObj = new SubTreeReduction(); // 18
        break;
    }
    case All_Greedy_LRU:
    {
        absMethodObj = new AllGreedyLRU();
        break;
    }
    case ALL_Greddy_LFU:
    {
        absMethodObj = new AllGreedyLFU();
        break;
    }
    default:
        break;
    }

    tool::traverse_dir(CmdLine.dirName, readfileList, nofilter);
    sort(readfileList.begin(), readfileList.end(), AbsMethod::compareNat);

    boost::thread *thTmp[2] = {nullptr};
    boost::thread::attributes attrs;
    attrs.set_stack_size(THREAD_STACK_SIZE);
    chunkerObj->SetOutputMQ(chunkerMQ);
    absMethodObj->SetInputMQ(chunkerMQ);
    absMethodObj->dataWrite_ = new dataWrite();
    const bool incrementalDesign4 = CmdLine.offlineMethod == Design4_;
    const bool incrementalDesign5 = CmdLine.offlineMethod == Design5_;
    const bool incrementalOffline = incrementalDesign4 || incrementalDesign5;
    if (incrementalOffline)
    {
        if (CmdLine.compressionMethod != ODESS)
        {
            cerr << "Incremental Design4/Design5 offline processing currently requires ODESS as the online method." << endl;
            return 1;
        }
        ResetDirectory("./InlineContainers");
        ResetDirectory("./OfflineContainers");
        ResetDirectory("./InlineContainers/current");
        if (incrementalDesign5)
        {
            ResetDirectory("./OfflineContainers/current");
        }
        absMethodObj->dataWrite_->setContainerPath("./InlineContainers/current/");
    }
    absMethodObj->AcceptThreshold = CmdLine.AcceptThreshold;
    absMethodObj->IsFalseFilter = CmdLine.IsFalseFilter;
    absMethodObj->TurnOnNameHash = CmdLine.TurnOnNameHash;
    chunkerObj->MULTI_HEADER_CHUNK = CmdLine.MultiHeaderChunk;

    auto startsum = std::chrono::high_resolution_clock::now();
    double MTarTime = 0;
    double incrementalOfflineTime = 0;
    size_t compactedChunkBoundary = 0;
    GCMarkState currentGCMarkState;
    if (CmdLine.chunkingType == MTAR || CmdLine.chunkingType == MTAROdess || CmdLine.chunkingType == MTARPalantir)
    {
        chunkerObj->MTar(readfileList, CmdLine.backupNum);
    }
    for (auto i = 0; i < CmdLine.backupNum; i++)
    {
        auto startTmp = std::chrono::high_resolution_clock::now();
        // set backup name
        chunkerObj->LoadChunkFile(readfileList[i]);
        absMethodObj->SetFilename(readfileList[i]);
        absMethodObj->dataWrite_->SetFilename(readfileList[i]);
        // thread running
        // if (chunkingType == TAR_MultiHeader)
        // {
        //     chunkerObj->SetHeaderChunkSize(uint64_t(ratio));
        // }
        thTmp[0] = new boost::thread(attrs, boost::bind(&Chunker::Chunking, chunkerObj));
        thTmp[1] = new boost::thread(attrs, boost::bind(&AbsMethod::ProcessTrace, absMethodObj));
        for (auto it : thTmp)
        {
            it->join();
        }
        // chunkerObj->WriteBoundariesToFile();
        // chunkerObj->ClearBoundaries();
        for (auto it : thTmp)
        {
            delete it;
        }
        auto endTmp = std::chrono::high_resolution_clock::now();
        auto TimeTmp = std::chrono::duration_cast<std::chrono::duration<double>>(endTmp - startTmp).count();
        if (CmdLine.compressionMethod != 5)

        {
            if (CmdLine.chunkingType == MTAR)
            {
                absMethodObj->Version_log(TimeTmp + chunkerObj->MTarTime[i]);
                MTarTime += chunkerObj->MTarTime[i];
            }
            else
                absMethodObj->Version_log(TimeTmp);
        }
        else
            absMethodObj->Version_log(TimeTmp, chunkerObj->ChunkTime.count());

        const std::vector<std::string> processedBackups(readfileList.begin(), readfileList.begin() + i + 1);
        currentGCMarkState = BuildGCMarkState(*absMethodObj->dataWrite_, processedBackups, CmdLine.retentionBackups);
        if (incrementalOffline)
        {
            size_t keptChunkCount = 0;
            size_t expiredChunkCount = 0;
            for (uint8_t keep : currentGCMarkState.keepChunk)
            {
                if (keep != 0)
                    keptChunkCount++;
                else
                    expiredChunkCount++;
            }
            cout << "----------------------gc mark-------------------------" << std::endl;
            cout << "retention backups: " << ResolveRetentionWindow(CmdLine.retentionBackups, processedBackups.size()) << std::endl;
            cout << "kept backups: " << currentGCMarkState.keptBackups.size() << std::endl;
            cout << "expired backups: " << currentGCMarkState.expiredBackups.size() << std::endl;
            cout << "kept chunks: " << keptChunkCount << std::endl;
            cout << "expired chunks: " << expiredChunkCount << std::endl;
        }

        if (incrementalOffline)
        {
            if (absMethodObj->rootChunkMap == nullptr || absMethodObj->dataWrite_->versionEndPoints.empty())
            {
                cerr << "Incremental Design4/Design5 offline processing requires a valid rootChunkMap and versionEndPoints." << endl;
                return 1;
            }

            const size_t currentVersionEnd = absMethodObj->dataWrite_->versionEndPoints.back();
            if (OfflineAbsMethodObj != nullptr && currentVersionEnd == compactedChunkBoundary)
            {
                cout << "----------------------incremental offline-------------------------" << std::endl;
                cout << "batch " << i << " has no new unique chunks, reuse the current offline archive" << std::endl;
                continue;
            }
            auto offlineStart = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> restoreChunkTimeBefore = std::chrono::duration<double>::zero();

            if (incrementalDesign4)
            {
                // Only the previous generation is needed for the next incremental rebuild,
                // so reuse two slots instead of retaining one directory per batch.
                const string offlineGenerationPath = "./OfflineContainers/gen" + to_string(i % 2) + "/";
                ResetDirectory(offlineGenerationPath);

                auto *nextOfflineMethod = new Design4();
                nextOfflineMethod->TREE_INSERT_SAVE_THRESHOLD = CmdLine.Threshold;
                nextOfflineMethod->dataWrite_ = absMethodObj->dataWrite_;
                nextOfflineMethod->rootChunkMap = absMethodObj->rootChunkMap;
                nextOfflineMethod->SetGCMarkState(&currentGCMarkState);
                nextOfflineMethod->offline_dataWrite_ = new dataWrite();
                nextOfflineMethod->offline_dataWrite_->setContainerPath(offlineGenerationPath);

                auto *nextDesign4 = static_cast<Design4 *>(nextOfflineMethod);
                if (OfflineAbsMethodObj != nullptr && OfflineAbsMethodObj->offline_dataWrite_ != nullptr && compactedChunkBoundary > 0)
                {
                    nextDesign4->SetHistoricalSource(OfflineAbsMethodObj->offline_dataWrite_, compactedChunkBoundary);
                }

                nextOfflineMethod->ProcessTrace();
                nextOfflineMethod->offline_dataWrite_->ProcessLastContainer();
                nextDesign4->SetHistoricalSource(nullptr, 0);

                if (OfflineAbsMethodObj != nullptr)
                {
                    if (OfflineAbsMethodObj->offline_dataWrite_ != nullptr)
                    {
                        delete OfflineAbsMethodObj->offline_dataWrite_;
                        OfflineAbsMethodObj->offline_dataWrite_ = nullptr;
                    }
                    OfflineAbsMethodObj->rootChunkMap = nullptr;
                    delete OfflineAbsMethodObj;
                }

                OfflineAbsMethodObj = nextOfflineMethod;
            }
            else
            {
                if (OfflineAbsMethodObj == nullptr)
                {
                    auto *appendOfflineMethod = new Design5();
                    appendOfflineMethod->offline_dataWrite_ = new dataWrite();
                    appendOfflineMethod->offline_dataWrite_->setContainerPath("./OfflineContainers/current/");
                    OfflineAbsMethodObj = appendOfflineMethod;
                }

                OfflineAbsMethodObj->TREE_INSERT_SAVE_THRESHOLD = CmdLine.Threshold;
                OfflineAbsMethodObj->dataWrite_ = absMethodObj->dataWrite_;
                OfflineAbsMethodObj->rootChunkMap = absMethodObj->rootChunkMap;
                OfflineAbsMethodObj->SetGCMarkState(&currentGCMarkState);
                restoreChunkTimeBefore = OfflineAbsMethodObj->RestoreChunkTime;

                auto *nextDesign5 = static_cast<Design5 *>(OfflineAbsMethodObj);
                nextDesign5->SetAppendRange(compactedChunkBoundary, currentVersionEnd);
                OfflineAbsMethodObj->ProcessTrace();
                OfflineAbsMethodObj->offline_dataWrite_->ProcessLastContainer();
            }

            auto offlineEnd = std::chrono::high_resolution_clock::now();
            auto offlineBatchTime = std::chrono::duration_cast<std::chrono::duration<double>>(offlineEnd - offlineStart).count();
            incrementalOfflineTime += offlineBatchTime;
            const auto restoreChunkTimeDelta = OfflineAbsMethodObj->RestoreChunkTime - restoreChunkTimeBefore;

            cout << "----------------------incremental offline-------------------------" << std::endl;
            cout << "batch " << i << " processed" << std::endl;
            cout << "RestoreChunkTime: " << restoreChunkTimeDelta.count() << "s" << std::endl;
            cout << "Time taken by incremental offline: " << offlineBatchTime << " s " << std::endl;
            cout << "Offline Compression ratio " << (double)absMethodObj->logicalchunkSize / (double)OfflineAbsMethodObj->uniquechunkSize << std::endl;
            cout << "Offline Throughput " << (double)absMethodObj->logicalchunkSize / offlineBatchTime / 1024 / 1024 << " MiB/s" << std::endl;

            compactedChunkBoundary = currentVersionEnd;
        }
    }

    auto endsum = std::chrono::high_resolution_clock::now();
    auto sumTimeInSeconds = std::chrono::duration_cast<std::chrono::duration<double>>(endsum - startsum).count();
    std::cout << "Time taken by for loop: " << sumTimeInSeconds << " s " << std::endl;
    if (CmdLine.chunkingType == MTAR)
        sumTimeInSeconds += MTarTime;
    tool::Logging(myName.c_str(), "logical Chunk Num is %d\n", absMethodObj->logicalchunkNum);
    tool::Logging(myName.c_str(), "unique Chunk Num is %d\n", absMethodObj->uniquechunkNum);
    tool::Logging(myName.c_str(), "Total logical size is %lu\n", absMethodObj->logicalchunkSize);
    tool::Logging(myName.c_str(), "Total compressed size is %lu\n", absMethodObj->uniquechunkSize);
    tool::Logging(myName.c_str(), "Compression ratio is %.4f\n", (double)absMethodObj->logicalchunkSize / (double)absMethodObj->uniquechunkSize);

    cout << "logical read bytes: " << absMethodObj->dataWrite_->logicalReadBytes << endl;
    cout << "physical read bytes: " << absMethodObj->dataWrite_->physicalReadBytes << endl;
    cout << "read amplification: " << (double)absMethodObj->dataWrite_->physicalReadBytes / absMethodObj->dataWrite_->logicalReadBytes << endl;

    if (CmdLine.compressionMethod != 5)
        absMethodObj->PrintChunkInfo(sumTimeInSeconds, CmdLine);
    else
        absMethodObj->PrintChunkInfo(sumTimeInSeconds, CmdLine, chunkerObj->ChunkTime.count());

    string fileName = "C" + to_string(CmdLine.chunkingType) + "_M" + to_string(CmdLine.compressionMethod);
    // absMethodObj->dataWrite_->Save_to_File_unique(fileName);
    absMethodObj->dataWrite_->ProcessLastContainer();

    // offline processing
    if (!incrementalOffline)
    {
        switch (CmdLine.offlineMethod)
        {
        case Offline_Greedy:
        {
            OfflineAbsMethodObj = new OfflineAllGreedy();
            break;
        }
        case Offline_Tree_Cut:
        {
            OfflineAbsMethodObj = new OfflineTreeCut();
            break;
        }
        case Offline_Tree_Cut_Layer:
        {
            OfflineAbsMethodObj = new OfflineTreeCutLayer();
            break;
        }
        case Offline_Tree_Cache:
        {
            OfflineAbsMethodObj = new OfflineTreeCache();
            break;
        }
        case Offline_Tree_Feature:
        {
            OfflineAbsMethodObj = new OfflineTreeFeature();
            break;
        }
        case Offline_Tree_Cut_Layer_Ignore:
        {
            OfflineAbsMethodObj = new OfflineTreeCutLayerIgnore(); // 5
            break;
        }
        case Offline_Tree_Feature_LRU:
        {
            OfflineAbsMethodObj = new OfflineTreeFeatureLru(); // 6
            break;
        }
        case Greedy_:
        {
            OfflineAbsMethodObj = new Greedy();
            break;
        }
        case Design1_:
        {
            OfflineAbsMethodObj = new Design1();
            break;
        }
        case Design2_:
        {
            OfflineAbsMethodObj = new Design2();
            break;
        }
        case Design3_:
        {
            OfflineAbsMethodObj = new Design3();
            break;
        }
        case Design4_:
        {
            OfflineAbsMethodObj = new Design4();
            break;
        }
        case Design5_:
        {
            OfflineAbsMethodObj = new Design5();
            break;
        }
        default:
            break;
        }
        // OfflineAbsMethodObj->TREE_INSERT_SAVE_THRESHOLD = CmdLine.Threshold;
        if (CmdLine.offlineMethod >= 0)
        {
            OfflineAbsMethodObj->TREE_INSERT_SAVE_THRESHOLD = CmdLine.Threshold;

            OfflineAbsMethodObj->offline_dataWrite_ = new dataWrite();
            OfflineAbsMethodObj->dataWrite_ = absMethodObj->dataWrite_;
            OfflineAbsMethodObj->rootChunkMap = absMethodObj->rootChunkMap;
            absMethodObj->rootChunkMap = nullptr;
            ResetDirectory("./OfflineContainers");
            OfflineAbsMethodObj->offline_dataWrite_->setContainerPath("./OfflineContainers/");
            auto startTmp = std::chrono::high_resolution_clock::now();
            OfflineAbsMethodObj->ProcessTrace();
            OfflineAbsMethodObj->offline_dataWrite_->ProcessLastContainer();
            auto endTmp = std::chrono::high_resolution_clock::now();
            auto offlineTimeTmp = std::chrono::duration_cast<std::chrono::duration<double>>(endTmp - startTmp).count();
            cout << "RestoreChunkTime: " << OfflineAbsMethodObj->RestoreChunkTime.count() << "s" << std::endl;
            std::cout << "Time taken by for offline: " << offlineTimeTmp << " s " << std::endl;
            std::cout << "Offline Compression ratio " << (double)absMethodObj->logicalchunkSize / (double)OfflineAbsMethodObj->uniquechunkSize << std::endl;
            std::cout << "Offline Throughput " << (double)absMethodObj->logicalchunkSize / offlineTimeTmp / 1024 / 1024 << " MiB/s" << std::endl;
            OfflineAbsMethodObj->PrintOffline(offlineTimeTmp, CmdLine);
        }
    }
    else if (OfflineAbsMethodObj != nullptr)
    {
        cout << "RestoreChunkTime: " << OfflineAbsMethodObj->RestoreChunkTime.count() << "s" << std::endl;
        std::cout << "Time taken by incremental offline: " << incrementalOfflineTime << " s " << std::endl;
        std::cout << "Offline Compression ratio " << (double)absMethodObj->logicalchunkSize / (double)OfflineAbsMethodObj->uniquechunkSize << std::endl;
        std::cout << "Offline Throughput " << (double)absMethodObj->logicalchunkSize / incrementalOfflineTime / 1024 / 1024 << " MiB/s" << std::endl;
        OfflineAbsMethodObj->PrintOffline(incrementalOfflineTime, CmdLine);
    }

    if (CmdLine.enableRestore)
    {
        double RestoreTimeSum = 0;
        size_t restoreBeginIndex = 0;
        if (CmdLine.retentionBackups > 0 && static_cast<size_t>(CmdLine.retentionBackups) < readfileList.size())
        {
            restoreBeginIndex = readfileList.size() - static_cast<size_t>(CmdLine.retentionBackups);
        }

        if(CmdLine.offlineMethod >= 0)
        {
            OfflineAbsMethodObj->offline_dataWrite_->physicalReadBytes = 0;
            OfflineAbsMethodObj->offline_dataWrite_->logicalReadBytes = 0;
            OfflineAbsMethodObj->offline_dataWrite_->restoreIOTime = std::chrono::duration<double>(0);
            OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeTime = std::chrono::duration<double>(0);
            OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeCount = 0;
            OfflineAbsMethodObj->offline_dataWrite_->RecipeMap.clear();
            for (const auto &backup : currentGCMarkState.keptBackups)
            {
                auto recipeIt = absMethodObj->dataWrite_->RecipeMap.find(backup);
                if (recipeIt != absMethodObj->dataWrite_->RecipeMap.end())
                {
                    OfflineAbsMethodObj->offline_dataWrite_->RecipeMap[backup] = recipeIt->second;
                }
            }
        }
        else
        {
            absMethodObj->dataWrite_->physicalReadBytes = 0;
            absMethodObj->dataWrite_->logicalReadBytes = 0;
            absMethodObj->dataWrite_->restoreIOTime = std::chrono::duration<double>(0);
            absMethodObj->dataWrite_->restoreDecodeTime = std::chrono::duration<double>(0);
            absMethodObj->dataWrite_->restoreDecodeCount = 0;
        }

        for (size_t i = restoreBeginIndex; i < static_cast<size_t>(CmdLine.backupNum); i++)
        {
            // 先清空cache，保证本轮统计独立
            if (CmdLine.offlineMethod >= 0)
                OfflineAbsMethodObj->offline_dataWrite_->ClearContainerCache();
            else
                absMethodObj->dataWrite_->ClearContainerCache();

            uint64_t prevPhysicalRead = 0, prevLogicalRead = 0;
            if (CmdLine.offlineMethod >= 0)
            {
                prevPhysicalRead = OfflineAbsMethodObj->offline_dataWrite_->physicalReadBytes;
                prevLogicalRead = OfflineAbsMethodObj->offline_dataWrite_->logicalReadBytes;
            }
            else
            {
                prevPhysicalRead = absMethodObj->dataWrite_->physicalReadBytes;
                prevLogicalRead = absMethodObj->dataWrite_->logicalReadBytes;
            }

            auto startTmp = std::chrono::high_resolution_clock::now();
            if (CmdLine.offlineMethod >= 0)
            {
                OfflineAbsMethodObj->offline_dataWrite_->RecipeMap = absMethodObj->dataWrite_->RecipeMap;
                OfflineAbsMethodObj->offline_dataWrite_->restoreFile(readfileList[i]);
            }
            else
                absMethodObj->dataWrite_->restoreFile(readfileList[i]);
            auto endTmp = std::chrono::high_resolution_clock::now();
            auto TimeTmp = std::chrono::duration_cast<std::chrono::duration<double>>(endTmp - startTmp).count();
            RestoreTimeSum += TimeTmp;

            // 记录恢复后的物理/逻辑读字节
            uint64_t curPhysicalRead = 0, curLogicalRead = 0;
            if (CmdLine.offlineMethod >= 0)
            {
                curPhysicalRead = OfflineAbsMethodObj->offline_dataWrite_->physicalReadBytes;
                curLogicalRead = OfflineAbsMethodObj->offline_dataWrite_->logicalReadBytes;
            }
            else
            {
                curPhysicalRead = absMethodObj->dataWrite_->physicalReadBytes;
                curLogicalRead = absMethodObj->dataWrite_->logicalReadBytes;
            }

            uint64_t versionPhysicalRead = curPhysicalRead - prevPhysicalRead;
            uint64_t versionLogicalRead = curLogicalRead - prevLogicalRead;
            double versionReadAmplification = versionLogicalRead > 0 ? (double)versionPhysicalRead / versionLogicalRead : 0.0;

            cout << "----------------------restore-------------------------" << std::endl;
            cout << "Version " << i << endl;
            cout << "Restore time: " << TimeTmp << " s" << endl;
            cout << "Version logical read bytes: " << versionLogicalRead << endl;
            cout << "Version physical read bytes: " << versionPhysicalRead << endl;
            cout << "Version read amplification: " << versionReadAmplification << endl;
            if (CmdLine.offlineMethod >= 0)
            {
                cout << "before visit container: " << OfflineAbsMethodObj->offline_dataWrite_->single << std::endl;
                cout << "after visit container: " << OfflineAbsMethodObj->offline_dataWrite_->multi << std::endl;
                cout << "total visit container: " << OfflineAbsMethodObj->offline_dataWrite_->single + OfflineAbsMethodObj->offline_dataWrite_->multi << std::endl;
                cout << "IO in restore: " << OfflineAbsMethodObj->offline_dataWrite_->restoreIOTime.count() << " s " << std::endl;
                cout << "Decode time in restore: " << OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeTime.count() << " s " << std::endl;
                cout << "Decode count in restore: " << OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeCount << endl;
                cout << "decode count / restore chunk: " << (double)OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeCount / OfflineAbsMethodObj->offline_dataWrite_->restoreChunkNum << endl;
            }
            else
            {
                cout << "before visit container: " << absMethodObj->dataWrite_->single << std::endl;
                cout << "after visit container: " << absMethodObj->dataWrite_->multi << std::endl;
                cout << "total visit container: " << absMethodObj->dataWrite_->single + absMethodObj->dataWrite_->multi << std::endl;
                cout << "IO in restore: " << absMethodObj->dataWrite_->restoreIOTime.count() << " s " << std::endl;
                cout << "Decode time in restore: " << absMethodObj->dataWrite_->restoreDecodeTime.count() << " s " << std::endl;
                cout << "Decode count in restore: " << absMethodObj->dataWrite_->restoreDecodeCount << endl;
                cout << "decode count / restore chunk: " << (double)absMethodObj->dataWrite_->restoreDecodeCount / absMethodObj->dataWrite_->restoreChunkNum << endl;
            }
        }
        double overallReadAmplification;
        double overallPhysicalRead;
        double overallLogicalRead;
        std::chrono::duration<double> restoreIoTime;
        std::chrono::duration<double> restoreDecodeTime;
        int restoreDecodeCount;
        double avgDecode;
        if(CmdLine.offlineMethod >= 0)
        {
            overallPhysicalRead = OfflineAbsMethodObj->offline_dataWrite_->physicalReadBytes;
            overallLogicalRead = OfflineAbsMethodObj->offline_dataWrite_->logicalReadBytes;
            overallReadAmplification = overallLogicalRead > 0 ? (double)overallPhysicalRead / overallLogicalRead : 0.0;
            restoreIoTime = OfflineAbsMethodObj->offline_dataWrite_->restoreIOTime;
            restoreDecodeTime = OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeTime;
            restoreDecodeCount = OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeCount;
            avgDecode = (double)OfflineAbsMethodObj->offline_dataWrite_->restoreDecodeCount / OfflineAbsMethodObj->offline_dataWrite_->restoreChunkNum;
        }
        else
        {
            overallPhysicalRead = absMethodObj->dataWrite_->physicalReadBytes;
            overallLogicalRead = absMethodObj->dataWrite_->logicalReadBytes;
            overallReadAmplification = overallLogicalRead > 0 ? (double)overallPhysicalRead / overallLogicalRead : 0.0;
            restoreIoTime = absMethodObj->dataWrite_->restoreIOTime;
            restoreDecodeTime = absMethodObj->dataWrite_->restoreDecodeTime;
            restoreDecodeCount = absMethodObj->dataWrite_->restoreDecodeCount;
            avgDecode = (double)absMethodObj->dataWrite_->restoreDecodeCount / absMethodObj->dataWrite_->restoreChunkNum;
        }

        cout << "----------------------overall-------------------------" << std::endl;
        cout << "Time taken by restoreFile: " << RestoreTimeSum << " s " << std::endl;
        cout << "Avg Restore throughput: " << (double)absMethodObj->logicalchunkSize / RestoreTimeSum / 1024 / 1024 << " MiB/s" << endl;
        cout << "Overall read amplification: " << overallReadAmplification << endl;
        cout << "IO in restore: " << restoreIoTime.count() << " s " << std::endl;
        cout << "Decode time in restore: " << restoreDecodeTime.count() << " s " << std::endl;
        cout << "Decode count in restore: " << restoreDecodeCount << endl;
        cout << "decode count / restore chunk: " << avgDecode << endl;
    }

    cout << "----------------------inline container-------------------------" << std::endl;
    if (absMethodObj && absMethodObj->dataWrite_)
    {
        absMethodObj->dataWrite_->PrintMetrics();
    }
    else
    {
        cout << "inline container dataWrite_ is nullptr!" << std::endl;
    }
    cout << "----------------------offline container-------------------------" << std::endl;
    if (OfflineAbsMethodObj && OfflineAbsMethodObj->offline_dataWrite_)
    {
        OfflineAbsMethodObj->offline_dataWrite_->PrintMetrics();
    }
    else
    {
        cout << "offline container offline_dataWrite_ is nullptr!" << std::endl;
    }

    // clear
    if (incrementalOffline && OfflineAbsMethodObj)
    {
        OfflineAbsMethodObj->rootChunkMap = nullptr;
    }
    if (OfflineAbsMethodObj && OfflineAbsMethodObj->offline_dataWrite_)
    {
        delete OfflineAbsMethodObj->offline_dataWrite_;
        OfflineAbsMethodObj->offline_dataWrite_ = nullptr;
    }
    if (absMethodObj && absMethodObj->dataWrite_)
    {
        delete absMethodObj->dataWrite_;
        absMethodObj->dataWrite_ = nullptr;
    }
    delete chunkerObj;
    delete absMethodObj;
    if (OfflineAbsMethodObj)
        delete OfflineAbsMethodObj;
    return 0;
}
