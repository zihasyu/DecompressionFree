#ifndef XDELTA_ADD_EXTRACTOR_H
#define XDELTA_ADD_EXTRACTOR_H

#include <cstring>
#include <stdexcept>
#include <cstdint>
// 使用示例
#include <iostream>
#include <iomanip>

/**
 * 从VCDiff delta chunk中提取所有ADD指令的数据
 * @param delta_chunk   输入的delta chunk数据
 * @param chunk_size    delta chunk的大小
 * @param my_buffer     输出缓冲区起始位置
 * @return              写入my_buffer的字节数（即缓冲区向右偏移的字节数）
 */
inline uint64_t extractVCDiffAddData(const uint8_t *delta_chunk, uint64_t chunk_size, uint8_t *my_buffer)
{
    if (!delta_chunk || !my_buffer || chunk_size == 0)
    {
        throw std::invalid_argument("Invalid parameters");
    }

    const uint8_t *input = delta_chunk;
    const uint8_t *input_end = delta_chunk + chunk_size;
    uint8_t *write_pos = my_buffer;

    // 解码状态枚举（模拟xdelta3的解码状态）
    enum decode_state
    {
        DEC_VCHEAD = 0,
        DEC_HDRIND,
        DEC_SECONDID,
        DEC_TABLEN,
        DEC_NEAR,
        DEC_SAME,
        DEC_TABDAT,
        DEC_APPLEN,
        DEC_APPDAT,
        DEC_WININD,
        DEC_CPYLEN,
        DEC_CPYOFF,
        DEC_ENCLEN,
        DEC_TGTLEN,
        DEC_DELIND,
        DEC_DATALEN,
        DEC_INSTLEN,
        DEC_ADDRLEN,
        DEC_CKSUM,
        DEC_DATA
    } state = DEC_VCHEAD;

    // 解码变量
    uint8_t hdr_indicator = 0;
    uint8_t win_indicator = 0;
    uint64_t data_len = 0, inst_len = 0, addr_len = 0;

    // 读取变长整数
    auto readSize = [&input, input_end]() -> uint64_t
    {
        uint64_t result = 0;
        uint64_t shift = 0;

        while (input < input_end)
        {
            uint8_t byte = *input++;
            result |= (byte & 0x7F) << shift;
            if ((byte & 0x80) == 0)
                break;
            shift += 7;
            if (shift >= 64)
                throw std::runtime_error("Invalid size encoding");
        }
        return result;
    };

    auto readByte = [&input, input_end]() -> uint8_t
    {
        if (input >= input_end)
        {
            throw std::runtime_error("Unexpected end of input");
        }
        return *input++;
    };

    auto skip = [&input, input_end](uint64_t bytes)
    {
        if (input + bytes > input_end)
        {
            throw std::runtime_error("Unexpected end of input");
        }
        input += bytes;
    };

    // RFC3284默认指令表（简化版）
    auto getDefaultInstruction = [](uint8_t code) -> std::tuple<uint8_t, uint8_t, uint8_t, uint8_t>
    {
        if (code == 0)
            return {1, 0, 0, 0}; // RUN, size=0

        if (code >= 1 && code <= 18)
        {
            return {2, 0, code, 0}; // ADD, immediate size
        }

        if (code >= 19 && code <= 162)
        {
            // COPY instructions
            uint8_t mode_offset = code - 19;
            uint8_t mode = mode_offset / 16;
            uint8_t size = (mode_offset % 16) + 4;
            return {3 + mode, 0, size, 0};
        }

        if (code >= 163 && code <= 246)
        {
            // ADD+COPY double instructions
            uint8_t index = code - 163;
            uint8_t mode = index / 12;
            uint8_t sub_index = index % 12;
            uint8_t add_size = (sub_index / 3) + 1;
            uint8_t copy_size = (sub_index % 3) + 4;
            return {2, 3 + mode, add_size, copy_size};
        }

        if (code >= 247 && code <= 255)
        {
            // COPY+ADD double instructions
            uint8_t mode = code - 247;
            return {3 + mode, 2, 4, 1};
        }

        return {0, 0, 0, 0}; // NOOP
    };

    // 按照xd3_decode_input的状态机流程
    while (input < input_end)
    {
        switch (state)
        {
        case DEC_VCHEAD:
        {
            // 验证VCDIFF魔数 - 精确按照源码逻辑
            if (input + 4 > input_end)
            {
                throw std::runtime_error("Incomplete VCDIFF header");
            }

            if (input[0] != 0xd6 || input[1] != 0xc3 || input[2] != 0xc4)
            {
                throw std::runtime_error("not a VCDIFF input");
            }

            if (input[3] != 0)
            {
                throw std::runtime_error("VCDIFF input version > 0 is not supported");
            }

            input += 4;
            state = DEC_HDRIND;
            break;
        }

        case DEC_HDRIND:
        {
            hdr_indicator = readByte();

            // VCD_INVHDR check
            if ((hdr_indicator & ~0x7U) != 0)
            {
                throw std::runtime_error("unrecognized header indicator bits set");
            }

            state = DEC_SECONDID;
            break;
        }

        case DEC_SECONDID:
        {
            if (hdr_indicator & 0x01)
            { // VCD_SECONDARY
                uint8_t secondid = readByte();
                // 简化处理，跳过secondary compressor
            }
            state = DEC_TABLEN;
            break;
        }

        case DEC_TABLEN:
        {
            if (hdr_indicator & 0x02)
            { // VCD_CODETABLE
                uint64_t codetblsz = readSize();
                if (codetblsz <= 2)
                {
                    throw std::runtime_error("invalid code table size");
                }
                // 跳过代码表数据
                skip(codetblsz);
            }
            else
            {
                // 使用默认表 - 这是我们要的情况
            }
            state = DEC_APPLEN;
            break;
        }

        case DEC_APPLEN:
        {
            if (hdr_indicator & 0x04)
            { // VCD_APPHEADER
                uint64_t appheadsz = readSize();
                skip(appheadsz);
            }
            state = DEC_WININD;
            break;
        }

        case DEC_WININD:
        {
            win_indicator = readByte();

            if ((win_indicator & ~0x7U) != 0)
            {
                throw std::runtime_error("unrecognized window indicator bits set");
            }

            state = DEC_CPYLEN;
            break;
        }

        case DEC_CPYLEN:
        {
            if (win_indicator & 0x01)
            { // VCD_SOURCE
                uint64_t cpylen = readSize();
                uint64_t cpyoff = readSize();
            }
            state = DEC_ENCLEN;
            break;
        }

        case DEC_ENCLEN:
        {
            uint64_t enclen = readSize();
            state = DEC_TGTLEN;
            break;
        }

        case DEC_TGTLEN:
        {
            uint64_t tgtlen = readSize();
            state = DEC_DELIND;
            break;
        }

        case DEC_DELIND:
        {
            uint8_t del_ind = readByte();
            state = DEC_DATALEN;
            break;
        }

        case DEC_DATALEN:
        {
            data_len = readSize();
            state = DEC_INSTLEN;
            break;
        }

        case DEC_INSTLEN:
        {
            inst_len = readSize();
            state = DEC_ADDRLEN;
            break;
        }

        case DEC_ADDRLEN:
        {
            addr_len = readSize();
            state = DEC_CKSUM;
            break;
        }

        case DEC_CKSUM:
        {
            if (win_indicator & 0x04)
            {            // VCD_ADLER32
                skip(4); // 跳过校验和
            }
            state = DEC_DATA;
            break;
        }

        case DEC_DATA:
        {
            // 现在我们到达了关键部分：处理DATA、INST、ADDR段
            const uint8_t *data_start = input;
            const uint8_t *inst_start = input + data_len;
            const uint8_t *addr_start = inst_start + inst_len;
            const uint8_t *next_window = addr_start + addr_len;

            // 检查边界
            if (next_window > input_end)
            {
                throw std::runtime_error("Window sections exceed input");
            }

            // 处理指令段，提取ADD数据 - 这里是核心逻辑
            const uint8_t *inst_ptr = inst_start;
            const uint8_t *inst_end = addr_start;
            uint64_t data_pos = 0;

            // 模拟xd3_decode_emit中的指令处理循环
            while (inst_ptr < inst_end)
            {
                if (inst_ptr >= inst_end)
                    break;

                uint8_t opcode = *inst_ptr++;
                auto [type1, type2, size1, size2] = getDefaultInstruction(opcode);

                // 处理第一个半指令 - 模拟xd3_decode_output_halfinst
                if (type1 != 0)
                {
                    uint64_t take = size1;

                    // 如果size为0，需要从指令流读取
                    if (take == 0 && inst_ptr < inst_end)
                    {
                        take = 0;
                        uint64_t shift = 0;
                        while (inst_ptr < inst_end)
                        {
                            uint8_t byte = *inst_ptr++;
                            take |= (byte & 0x7F) << shift;
                            if ((byte & 0x80) == 0)
                                break;
                            shift += 7;
                        }
                    }

                    if (type1 == 2)
                    { // XD3_ADD - 关键！
                        // 严格按照源码的边界检查
                        if (data_pos + take > data_len)
                        {
                            throw std::runtime_error("data underflow");
                        }

                        // 直接复制ADD数据 - 模拟memcpy逻辑
                        std::memcpy(write_pos, data_start + data_pos, take);
                        write_pos += take;
                        data_pos += take;
                    }
                    else if (type1 == 1)
                    { // XD3_RUN
                        if (data_pos + 1 > data_len)
                        {
                            throw std::runtime_error("data underflow");
                        }
                        data_pos += 1; // RUN只消耗1字节
                    }
                    // COPY (type1 >= 3) 不消耗DATA段
                }

                // 处理第二个半指令
                if (type2 != 0)
                {
                    uint64_t take = size2;

                    if (take == 0 && inst_ptr < inst_end)
                    {
                        take = 0;
                        uint64_t shift = 0;
                        while (inst_ptr < inst_end)
                        {
                            uint8_t byte = *inst_ptr++;
                            take |= (byte & 0x7F) << shift;
                            if ((byte & 0x80) == 0)
                                break;
                            shift += 7;
                        }
                    }

                    if (type2 == 2)
                    { // XD3_ADD
                        if (data_pos + take > data_len)
                        {
                            throw std::runtime_error("data underflow");
                        }

                        std::memcpy(write_pos, data_start + data_pos, take);
                        write_pos += take;
                        data_pos += take;
                    }
                    else if (type2 == 1)
                    { // XD3_RUN
                        if (data_pos + 1 > data_len)
                        {
                            throw std::runtime_error("data underflow");
                        }
                        data_pos += 1;
                    }
                }
            }

            // 移动到下一个窗口
            input = next_window;
            state = DEC_WININD;
            break;
        }
        }
    }

    return write_pos - my_buffer;
}
#endif // XDELTA_ADD_EXTRACTOR_H
