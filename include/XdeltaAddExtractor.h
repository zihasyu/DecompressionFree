#ifndef XDELTA_ADD_EXTRACTOR_H
#define XDELTA_ADD_EXTRACTOR_H

#include <cstring>
#include <stdexcept>
#include <cstdint>
// 使用示例
#include <iostream>
#include <iomanip>
#include <stdint.h>
#include <stddef.h>

// 返回状态码
#define VCDIFF_OK 0
#define VCDIFF_NEED_INPUT 1
#define VCDIFF_PARSE_ERROR 2

typedef struct
{
    uint8_t *data_section_ptr;  // 指向Data Section开始位置
    size_t data_section_length; // Data Section长度
    size_t header_end_offset;   // Header结束位置（Data Section开始位置）
    int status;                 // 解析状态
} vcdiff_data_section_info;
/**
 * 从VCDiff delta chunk中提取所有ADD指令的数据
 * @param delta_chunk   输入的delta chunk数据
 * @param chunk_size    delta chunk的大小
 * @param my_buffer     输出缓冲区起始位置
 * @return              写入my_buffer的字节数（即缓冲区向右偏移的字节数）
 */

inline int decode_size_vlq(const uint8_t **input, size_t *avail, size_t *result)
{
    const uint8_t *p = *input;
    size_t val = 0;
    size_t consumed = 0;

    while (*avail > 0 && consumed < 5)
    { // VLQ最多5字节
        uint8_t byte = *p++;
        consumed++;
        (*avail)--;

        val = (val << 7) | (byte & 0x7F);

        if ((byte & 0x80) == 0)
        {
            *result = val;
            *input = p;
            return 0; // 成功
        }
    }

    return 1; // 需要更多输入
}

// 模仿xdelta3的解析方式
typedef struct
{
    const uint8_t *input_start; // 输入缓冲区开始
    const uint8_t *current_pos; // 当前解析位置
    size_t remaining;           // 剩余字节数

    // 解析出的信息
    uint8_t win_indicator;
    size_t source_len;
    size_t source_off;
    size_t delta_len;
    size_t data_sect_size;
    size_t inst_sect_size;
    size_t addr_sect_size;

    // Data Section的位置信息
    const uint8_t *data_sect_ptr;

} xd3_window_parser;

inline int parse_window_for_data_section(const uint8_t *input, size_t input_size,
                                         const uint8_t **data_ptr, size_t *data_len)
{
    xd3_window_parser parser = {0};
    parser.input_start = input;
    parser.current_pos = input;
    parser.remaining = input_size;

    if (parser.remaining < 1)
        return 1; // 需要更多输入

    // 1. Win_Indicator
    parser.win_indicator = *parser.current_pos++;
    parser.remaining--;

    // 2. 如果有SOURCE/TARGET，解析长度和偏移
    if (parser.win_indicator & 0x01)
    { // VCD_SOURCE
        if (decode_size_vlq(&parser.current_pos, &parser.remaining,
                            &parser.source_len) != 0)
        {
            return 1;
        }

        if (decode_size_vlq(&parser.current_pos, &parser.remaining,
                            &parser.source_off) != 0)
        {
            return 1;
        }
    }

    // 3. Delta encoding length
    if (decode_size_vlq(&parser.current_pos, &parser.remaining,
                        &parser.delta_len) != 0)
    {
        return 1;
    }

    // 4. Target length (跳过，但需要解析)
    size_t target_len;
    if (decode_size_vlq(&parser.current_pos, &parser.remaining,
                        &target_len) != 0)
    {
        return 1;
    }

    // 5. Delta indicator (跳过)
    if (parser.remaining < 1)
        return 1;
    parser.current_pos++;
    parser.remaining--;

    // 6. Data section length
    if (decode_size_vlq(&parser.current_pos, &parser.remaining,
                        &parser.data_sect_size) != 0)
    {
        return 1;
    }

    // 7. Instruction section length
    if (decode_size_vlq(&parser.current_pos, &parser.remaining,
                        &parser.inst_sect_size) != 0)
    {
        return 1;
    }

    // 8. Address section length
    if (decode_size_vlq(&parser.current_pos, &parser.remaining,
                        &parser.addr_sect_size) != 0)
    {
        return 1;
    }

    // 9. 跳过可能的校验和
    if (parser.win_indicator & 0x04)
    { // VCD_ADLER32
        if (parser.remaining < 4)
            return 1;
        parser.current_pos += 4;
        parser.remaining -= 4;
    }

    // 现在current_pos指向Data Section开始位置
    if (parser.remaining < parser.data_sect_size)
    {
        return 1; // 数据不完整
    }

    // 成功！返回零拷贝信息
    *data_ptr = parser.current_pos; // ← 这就是xdelta3的零拷贝方式
    *data_len = parser.data_sect_size;

    return 0;
}
#endif // XDELTA_ADD_EXTRACTOR_H
