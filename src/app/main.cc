#include <iostream>
#include <string>
#include <csignal>
#include <sstream>
#include <chrono>

#include "../../include/allmethod.h"

using namespace std;

void signalHandler(int signum)
{
    cout << "Interrupt signal (" << signum << ") received.\n";
    exit(signum);
}

int main(int argc, char **argv)
{
    signal(SIGINT, signalHandler);
    CommandLine_t CmdLine;
    // uint32_t chunkingType;
    // uint32_t compressionMethod;
    // uint32_t backupNum;
    // string dirName;
    string myName = "BiSearchSystem";
    CmdLine.ratio = 10;
    CmdLine.IsFalseFilter = true;
    CmdLine.AcceptThreshold = 0;
    CmdLine.TurnOnNameHash = true;
    CmdLine.MultiHeaderChunk = 16;

    vector<string> readfileList;

    const char optString[] = "i:m:c:n:r:a:b:t:H:";
    if (argc != sizeof(optString) && argc != sizeof(optString) - 2 && argc != sizeof(optString) - 4 && argc != sizeof(optString) - 6 && argc != sizeof(optString) - 8 && argc != sizeof(optString) - 10 && argc != sizeof(optString) - 12)
    {
        cout << "argc is " << argc << endl;
        cout << "Usage: " << argv[0] << " -i <input file> -m <chunking method> -c <compression method> -n <process number> -r <Bisearch fault ratio> -a <False Filter Fixed parameters> -b <0 = fixed parameter> -t <0 = No meta-guided> -H <Multi Header num>" << endl;
        return 0;
    }

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
        default:
            break;
        }
    }

    AbsMethod *absMethodObj;
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
        absMethodObj = new Odess();
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
    absMethodObj->AcceptThreshold = CmdLine.AcceptThreshold;
    absMethodObj->IsFalseFilter = CmdLine.IsFalseFilter;
    absMethodObj->TurnOnNameHash = CmdLine.TurnOnNameHash;
    chunkerObj->MULTI_HEADER_CHUNK = CmdLine.MultiHeaderChunk;

    // new design
    // if (chunkingType == TAR_MultiHeader)
    // {
    //     MessageQueue<uint64_t> *MaskMQ = new MessageQueue<uint64_t>(CHUNK_QUEUE_SIZE);
    //     chunkerObj->SetOutputMaskMQ(MaskMQ);
    //     absMethodObj->SetInputMaskMQ(MaskMQ);
    // }

    auto startsum = std::chrono::high_resolution_clock::now();
    double MTarTime = 0;
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
    }

    auto endsum = std::chrono::high_resolution_clock::now();
    auto sumTime = (endsum - startsum);
    auto sumTimeInSeconds = std::chrono::duration_cast<std::chrono::seconds>(endsum - startsum).count();
    std::cout << "Time taken by for loop: " << sumTimeInSeconds << " s " << std::endl;
    if (CmdLine.chunkingType == MTAR)
        sumTimeInSeconds += MTarTime;
    tool::Logging(myName.c_str(), "logical Chunk Num is %d\n", absMethodObj->logicalchunkNum);
    tool::Logging(myName.c_str(), "unique Chunk Num is %d\n", absMethodObj->uniquechunkNum);
    tool::Logging(myName.c_str(), "Total logical size is %lu\n", absMethodObj->logicalchunkSize);
    tool::Logging(myName.c_str(), "Total compressed size is %lu\n", absMethodObj->uniquechunkSize);
    tool::Logging(myName.c_str(), "Compression ratio is %.4f\n", (double)absMethodObj->logicalchunkSize / (double)absMethodObj->uniquechunkSize);
    // if (compressionMethod != 5)
    //     absMethodObj->PrintChunkInfo(dirName, chunkingType, compressionMethod, backupNum, sumTimeInSeconds, ratio, AcceptThreshold, IsFalseFilter);
    // else
    //     absMethodObj->PrintChunkInfo(dirName, chunkingType, compressionMethod, backupNum, sumTimeInSeconds, ratio, chunkerObj->ChunkTime.count(), AcceptThreshold, IsFalseFilter);
    if (CmdLine.compressionMethod != 5)
        absMethodObj->PrintChunkInfo(sumTimeInSeconds, CmdLine);
    else
        absMethodObj->PrintChunkInfo(sumTimeInSeconds, CmdLine, chunkerObj->ChunkTime.count());

    string fileName = "C" + to_string(CmdLine.chunkingType) + "_M" + to_string(CmdLine.compressionMethod);
    // absMethodObj->dataWrite_->Save_to_File_unique(fileName);
    delete absMethodObj->dataWrite_;
    delete chunkerObj;
    delete absMethodObj;
    return 0;
}