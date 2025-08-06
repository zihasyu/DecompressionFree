/*
 * @Author: Helix0503 834991203@qq.com
 * @Date: 2024-01-08 16:41:48
 * @LastEditors: Helix && 834991203@qq.com
 * @LastEditTime: 2024-01-31 21:04:39
 * @FilePath: /LocalDedupSim/include/struct.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#ifndef MY_STRUCT_H
#define MY_STRUCT_H

#include "define.h"
#include <cstdint>
#include <vector>
#include <unordered_map>

typedef struct Chunk_t
{
    uint64_t chunkID;             // 8 bytes
    uint64_t chunkSize;           // 8 bytes
    uint64_t saveSize;            // 8 bytes
    uint64_t offset;              // 8 bytes
    uint64_t containerID;         // 8 bytes
    uint8_t *chunkPtr;            // 8 bytes (assuming 64-bit pointer)
    uint64_t name = 0;            // 8 bytes
    int basechunkID;              // 4 bytes
    uint8_t deltaFlag = NO_DELTA; // 1 byte
    bool loadFromDisk = false;    // 1 byte
    bool HeaderFlag = false;      // 1 byte
    bool NameExist = true;        // 1 byte
    // 1 byte padding to align the structure to 8 bytes boundary

    uint64_t firstChildID = UINT64_MAX;
    uint64_t nextSiblingID = UINT64_MAX;
} Chunk_t;

typedef struct
{
    uint64_t chunkSize;           // 8 bytes
    uint64_t saveSize;            // 8 bytes
    uint64_t offset;              // 8 bytes
    uint64_t containerID;         // 8 bytes
    uint8_t *chunkPtr;            // 8 bytes
    std::string basechunkFP;      // 32bytes
    uint8_t deltaFlag = NO_DELTA; // 8 bytes
    bool loadFromDisk = false;    // 1 bytes
    // bool HeaderFlag = false;
    //  bool NameExist = true;
    // offset and containerId
} Chunk_t_odess;

typedef struct
{
    uint64_t size;
    uint64_t chunkNum;
    uint64_t containerID;
    uint8_t data[CONTAINER_MAX_SIZE];
} Container_t;

typedef struct
{
    uint64_t headerSegmentId;
    uint64_t dataSegmentId;
    uint64_t blockTypeMask;
} RecipeSeg_t;

typedef uint64_t Recipe_t;

typedef uint64_t Recipe_Header_t;

typedef struct
{
    std::string dirName;
    int chunkingType;
    int compressionMethod;
    int backupNum;
    double ratio;
    double AcceptThreshold;
    bool IsFalseFilter;
    bool TurnOnNameHash;
    int MultiHeaderChunk = 16; // num
} CommandLine_t;

#endif