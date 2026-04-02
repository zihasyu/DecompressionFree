#include "../../include/gc_simulator.h"

using namespace std;

void GCSimulator::Init(dataWrite *inline_dw, dataWrite *offline_dw,
                       int total_versions, int deleted_versions,
                       const vector<string> &file_list) {
  inline_dw_ = inline_dw;
  offline_dw_ = offline_dw;
  total_versions_ = total_versions;
  deleted_versions_ = deleted_versions;
  file_list_ = file_list;

  alive_logical_chunks_.clear();
  pinned_alive_chunks_.clear();
  all_chunk_ids_.clear();
  logical_deleted_size_ = 0;
  physical_reclaimable_size_ = 0;
  pinned_size_ = 0;

  // 收集全系统的 Chunk ID 集合 (来自 offline chunklist)
  dataWrite *target_dw = (offline_dw_ != nullptr) ? offline_dw_ : inline_dw_;
  for (size_t i = 0; i < target_dw->chunklist.size(); i++) {
    all_chunk_ids_.insert(i);
  }

  cout << "========== GC Simulator Init ==========" << endl;
  cout << "Total versions: " << total_versions_ << endl;
  cout << "Deleted versions (oldest): " << deleted_versions_ << endl;
  cout << "Retained versions: " << (total_versions_ - deleted_versions_)
       << endl;
  cout << "Total chunks in system: " << all_chunk_ids_.size() << endl;
  cout << "=======================================" << endl;
}

void GCSimulator::LogicalMarkPhase() {
  cout << "--- [GC Phase 1] Logical Mark ---" << endl;

  // 存活版本：从 deleted_versions_ 开始到 total_versions_-1
  // 遍历存活版本的 Recipe，标记 alive chunks
  // RecipeMap: filename -> vector<Recipe_t(uint64_t)>
  // file_list_ 按版本顺序排列

  for (int v = deleted_versions_; v < total_versions_; v++) {
    const string &filename = file_list_[v];
    // 查找该文件的 Recipe
    auto it = inline_dw_->RecipeMap.find(filename);
    if (it != inline_dw_->RecipeMap.end()) {
      for (uint64_t chunkID : it->second) {
        alive_logical_chunks_.insert(chunkID);
      }
    }
  }

  // 计算逻辑上应释放的空间
  dataWrite *target_dw = (offline_dw_ != nullptr) ? offline_dw_ : inline_dw_;
  uint64_t alive_count = 0;
  uint64_t dead_count = 0;
  for (uint64_t cid : all_chunk_ids_) {
    if (alive_logical_chunks_.find(cid) == alive_logical_chunks_.end()) {
      // 逻辑死亡的 chunk
      Chunk_t meta = target_dw->Get_Chunk_MetaInfo(cid);
      logical_deleted_size_ += meta.saveSize;
      dead_count++;
    } else {
      alive_count++;
    }
  }

  cout << "Alive logical chunks: " << alive_count << endl;
  cout << "Dead logical chunks: " << dead_count << endl;
  cout << "Logical deleted size: " << logical_deleted_size_ << " bytes ("
       << (double)logical_deleted_size_ / (1024.0 * 1024.0) << " MB)" << endl;
}

void GCSimulator::TracePinChain(uint64_t chunk_id) {
  dataWrite *target_dw = (offline_dw_ != nullptr) ? offline_dw_ : inline_dw_;

  // 如果已经在 alive 或 pinned 集合中，无需再追溯
  if (alive_logical_chunks_.count(chunk_id) ||
      pinned_alive_chunks_.count(chunk_id))
    return;

  // 检查该 chunk 是否存在于系统中
  if (chunk_id >= target_dw->chunklist.size())
    return;

  // 该块逻辑上已死（不在 alive 中），但现在被依赖了，所以 Pin 住
  pinned_alive_chunks_.insert(chunk_id);

  // 继续追溯：该 pinned 块自身如果也是 Delta，则其 base 也须 Pin
  Chunk_t meta = target_dw->Get_Chunk_MetaInfo(chunk_id);
  if (meta.deltaFlag == DELTA && meta.basechunkID >= 0) {
    TracePinChain(static_cast<uint64_t>(meta.basechunkID));
  }
}

void GCSimulator::PhysicalPinTracePhase() {
  cout << "--- [GC Phase 2] Physical Pin Trace ---" << endl;

  dataWrite *target_dw = (offline_dw_ != nullptr) ? offline_dw_ : inline_dw_;

  // 遍历所有活块，追溯其 Delta 依赖链
  for (uint64_t cid : alive_logical_chunks_) {
    if (cid >= target_dw->chunklist.size())
      continue;

    Chunk_t meta = target_dw->Get_Chunk_MetaInfo(cid);
    if (meta.deltaFlag == DELTA && meta.basechunkID >= 0) {
      uint64_t base_id = static_cast<uint64_t>(meta.basechunkID);
      // 如果 base 不在 alive 集合中，说明它是被 pin 的僵尸块
      TracePinChain(base_id);
    }
  }

  // 统计 pinned 块的总大小
  for (uint64_t cid : pinned_alive_chunks_) {
    Chunk_t meta = target_dw->Get_Chunk_MetaInfo(cid);
    pinned_size_ += meta.saveSize;
  }

  cout << "Pinned alive chunks (zombie base): " << pinned_alive_chunks_.size()
       << endl;
  cout << "Pinned size: " << pinned_size_ << " bytes ("
       << (double)pinned_size_ / (1024.0 * 1024.0) << " MB)" << endl;
}

void GCSimulator::EvaluateFragmentation() {
  cout << "--- [GC Phase 3] Fragmentation Evaluation ---" << endl;

  dataWrite *target_dw = (offline_dw_ != nullptr) ? offline_dw_ : inline_dw_;

  // ========= 1. LRPR (Logical-Physical Reclaim Ratio) =========
  // 实际可回收 = 逻辑死亡 - 被 Pin 住的
  physical_reclaimable_size_ = (logical_deleted_size_ > pinned_size_)
                                   ? (logical_deleted_size_ - pinned_size_)
                                   : 0;

  double lrpr =
      (logical_deleted_size_ > 0)
          ? (double)physical_reclaimable_size_ / (double)logical_deleted_size_
          : 0.0;

  cout << endl;
  cout << "========== LRPR Report ==========" << endl;
  cout << "Logical deleted size:       " << logical_deleted_size_ << " bytes ("
       << (double)logical_deleted_size_ / (1024.0 * 1024.0) << " MB)" << endl;
  cout << "Pinned (unreclaimable) size: " << pinned_size_ << " bytes ("
       << (double)pinned_size_ / (1024.0 * 1024.0) << " MB)" << endl;
  cout << "Physical reclaimable size:  " << physical_reclaimable_size_
       << " bytes (" << (double)physical_reclaimable_size_ / (1024.0 * 1024.0)
       << " MB)" << endl;
  cout << "LRPR (Physical/Logical):    " << lrpr * 100.0 << "%" << endl;
  cout << "=================================" << endl;

  // ========= 2. Container Liveness Distribution =========
  // 按 containerID 聚合
  unordered_map<uint64_t, uint64_t>
      container_total_size; // containerID -> 总占用字节
  unordered_map<uint64_t, uint64_t>
      container_alive_size; // containerID -> 活块占用字节

  for (size_t cid = 0; cid < target_dw->chunklist.size(); cid++) {
    Chunk_t &chunk = target_dw->chunklist[cid];
    uint64_t cont_id = chunk.containerID;

    container_total_size[cont_id] += chunk.saveSize;

    bool is_alive = alive_logical_chunks_.count(cid) > 0;
    bool is_pinned = pinned_alive_chunks_.count(cid) > 0;
    if (is_alive || is_pinned) {
      container_alive_size[cont_id] += chunk.saveSize;
    }
  }

  // 统计 Liveness 分布 (分桶: 0%, 0-10%, 10-20%, ..., 90-100%, 100%)
  int liveness_histogram[12] = {
      0}; // [0]=0%, [1]=0-10%, ..., [10]=90-100%, [11]=100%
  int total_containers = container_total_size.size();
  int fully_reclaimable_containers = 0;
  int fully_alive_containers = 0;

  for (auto &[cont_id, total] : container_total_size) {
    uint64_t alive = container_alive_size[cont_id]; // 默认 0 if not found
    double ratio = (total > 0) ? (double)alive / (double)total : 0.0;

    if (alive == 0) {
      liveness_histogram[0]++;
      fully_reclaimable_containers++;
    } else if (ratio >= 1.0) {
      liveness_histogram[11]++;
      fully_alive_containers++;
    } else {
      int bucket = (int)(ratio * 10.0) + 1; // 1-10 对应 0-10% 到 90-100%
      if (bucket > 10)
        bucket = 10;
      liveness_histogram[bucket]++;
    }
  }

  cout << endl;
  cout << "========== Container Liveness Distribution ==========" << endl;
  cout << "Total containers: " << total_containers << endl;
  cout << "Fully reclaimable (0% alive):   " << fully_reclaimable_containers
       << endl;
  cout << "Fully alive (100%):             " << fully_alive_containers << endl;
  cout << "----------------------------------------------------" << endl;
  cout << "  Liveness 0% (empty):    " << liveness_histogram[0] << endl;
  cout << "  Liveness 0-10%:         " << liveness_histogram[1] << endl;
  cout << "  Liveness 10-20%:        " << liveness_histogram[2] << endl;
  cout << "  Liveness 20-30%:        " << liveness_histogram[3] << endl;
  cout << "  Liveness 30-40%:        " << liveness_histogram[4] << endl;
  cout << "  Liveness 40-50%:        " << liveness_histogram[5] << endl;
  cout << "  Liveness 50-60%:        " << liveness_histogram[6] << endl;
  cout << "  Liveness 60-70%:        " << liveness_histogram[7] << endl;
  cout << "  Liveness 70-80%:        " << liveness_histogram[8] << endl;
  cout << "  Liveness 80-90%:        " << liveness_histogram[9] << endl;
  cout << "  Liveness 90-100%:       " << liveness_histogram[10] << endl;
  cout << "  Liveness 100% (full):   " << liveness_histogram[11] << endl;
  cout << "====================================================" << endl;

  // 关键碎片指标：有多少容器因为少量 pinned 块而无法回收
  int fragmented_containers = 0;
  for (auto &[cont_id, total] : container_total_size) {
    uint64_t alive = container_alive_size[cont_id];
    if (alive > 0 && alive < total) {
      double ratio = (double)alive / (double)total;
      if (ratio < 0.3) // 低于 30% 利用率
        fragmented_containers++;
    }
  }
  cout << endl;
  cout << "Severely fragmented containers (<30% alive): "
       << fragmented_containers << endl;
  cout << "Fragmentation ratio: "
       << (total_containers > 0
               ? (double)fragmented_containers / total_containers * 100.0
               : 0)
       << "%" << endl;

  // ========= 3. GC Compaction Write Amplification Factor =========
  // 对每个非全满容器，计算 Compact 它需要的 I/O 和回收的空间
  //   I/O cost = container_total (读) + container_alive (写搬活块)
  //   reclaimed = container_total - container_alive (释放的死块空间)
  //   WAF = total_io / total_reclaimed
  uint64_t gc_total_io = 0;
  uint64_t gc_total_reclaimed = 0;
  int compactable_containers = 0;

  for (auto &[cont_id, total] : container_total_size) {
    uint64_t alive = container_alive_size[cont_id];
    if (alive > 0 && alive < total) {
      // 非空且非全满的容器才需要 Compact
      uint64_t dead = total - alive;
      gc_total_io += total + alive; // 读整个容器 + 写出活块
      gc_total_reclaimed += dead;
      compactable_containers++;
    } else if (alive == 0) {
      // 全空容器：直接删除，零搬迁成本
      gc_total_reclaimed += total;
      // I/O cost = 0 (直接删元数据即可)
    }
  }

  double gc_waf = (gc_total_reclaimed > 0)
                      ? (double)gc_total_io / (double)gc_total_reclaimed
                      : 0.0;

  cout << endl;
  cout << "========== GC Compaction Cost ==========" << endl;
  cout << "Compactable containers:     " << compactable_containers << endl;
  cout << "Total I/O for compaction:   " << gc_total_io << " bytes ("
       << (double)gc_total_io / (1024.0 * 1024.0) << " MB)" << endl;
  cout << "Total reclaimable by compaction: " << gc_total_reclaimed
       << " bytes (" << (double)gc_total_reclaimed / (1024.0 * 1024.0) << " MB)"
       << endl;
  cout << "GC Compaction WAF:          " << gc_waf << endl;
  cout << "========================================" << endl;
}

void GCSimulator::RunAll() {
  cout << endl;
  cout << "############################################" << endl;
  cout << "#      GC Simulator - Full Evaluation      #" << endl;
  cout << "############################################" << endl;

  LogicalMarkPhase();
  PhysicalPinTracePhase();
  EvaluateFragmentation();

  cout << endl;
  cout << "############################################" << endl;
  cout << "#      GC Simulation Complete              #" << endl;
  cout << "############################################" << endl;
}
