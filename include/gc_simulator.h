#ifndef GC_SIMULATOR_H
#define GC_SIMULATOR_H

#include "datawrite.h"
#include "define.h"
#include "struct.h"
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

/**
 * @brief GCSimulator: 旁路诊断模块，用于验证在增量压缩系统中
 *        逻辑删除备份后，由于 Delta 依赖链导致的"物理锁定 (Pinning)"问题。
 *
 *        该模块不执行任何物理数据搬移，仅通过元数据分析来量化：
 *        1. 逻辑上应释放但物理上无法释放的 Chunk 数量与空间
 *        2. 容器碎片化分布 (Container Liveness Distribution)
 */
class GCSimulator {
public:
  GCSimulator() = default;
  ~GCSimulator() = default;

  /**
   * @brief 初始化 GC 模拟器
   * @param inline_dw   Inline 阶段的 dataWrite 指针 (持有 RecipeMap 和 inline
   * chunklist)
   * @param offline_dw  Offline 阶段的 dataWrite 指针 (持有 offline chunklist
   * 和容器)
   * @param total_versions   总共备份的版本数
   * @param deleted_versions 模拟删除的最老版本数（例如删除前 20 个）
   * @param file_list        按顺序排列的备份文件路径列表
   */
  void Init(dataWrite *inline_dw, dataWrite *offline_dw, int total_versions,
            int deleted_versions, const std::vector<std::string> &file_list);

  /**
   * @brief 阶段 1: 逻辑标记
   *        遍历存活版本的 Recipe，收集所有被活跃备份直接引用的 Chunk ID
   */
  void LogicalMarkPhase();

  /**
   * @brief 阶段 2: 物理锁定溯源
   *        对存活的 Delta Chunk 递归追溯 basechunkID，
   *        将逻辑已死但被依赖的 Base Chunk 标记为 Pinned
   */
  void PhysicalPinTracePhase();

  /**
   * @brief 阶段 3: 碎片化评估与统计输出
   *        计算 LRPR、容器 Liveness 分布，输出报告
   */
  void EvaluateFragmentation();

  /**
   * @brief 一键执行全部三个阶段
   */
  void RunAll();

private:
  // 数据源
  dataWrite *inline_dw_ = nullptr; // Inline 阶段 dataWrite (持有 RecipeMap)
  dataWrite *offline_dw_ =
      nullptr; // Offline 阶段 dataWrite (持有 offline chunklist + containers)
  int total_versions_ = 0;
  int deleted_versions_ = 0;
  std::vector<std::string> file_list_;

  // 标记集合
  std::unordered_set<uint64_t>
      alive_logical_chunks_; // 被存活 Recipe 直接引用的 Chunk ID
  std::unordered_set<uint64_t>
      pinned_alive_chunks_; // 逻辑已死但被物理依赖锁定的 Chunk ID
  std::unordered_set<uint64_t> all_chunk_ids_; // 全系统的 Chunk ID 集合

  // 统计
  uint64_t logical_deleted_size_ = 0;      // 逻辑上应释放的总字节
  uint64_t physical_reclaimable_size_ = 0; // 实际可物理回收的字节
  uint64_t pinned_size_ = 0;               // 被 Pin 住的僵尸块总字节

  /**
   * @brief 递归追溯一个 Chunk 的 basechunkID 链，
   *        将链上所有逻辑已死的祖先标记为 Pinned
   */
  void TracePinChain(uint64_t chunk_id);
};

#endif // GC_SIMULATOR_H
