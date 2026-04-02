#验证压缩率 #mdelta

# 实验目标
备份存储系统中，存储优化往往引入跨版本的增量压缩），使得不同备份版本的数据块之间产生物理依赖关系。当系统进行逻辑删除时，被删除版本中的部分数据块虽然不再被任何有效备份直接引用，**却因被后续版本的增量块作为基准块而无法物理回收**，形成无法删除的僵尸块。这一现象在**多层增量压缩**下尤为严重——依赖链可横跨数十个备份版本——导致 GC 实际可回收空间远小于逻辑删除量，系统存储空间长期无法真正释放。

本实验旨在定量验证上述问题的严重程度，具体指标为：逻辑删除的空间与实际物理可回收空间之间的差距，以及容器层面的碎片化分布。
# 实验设计
## 工作流

1. **全量备份导入**：将数据集全量导入系统，经 Inline 去重与增量压缩，再经 MDelta 离线优化重排后写入 4MB 容器。
    
2. **模拟备份轮转（Backup Turnover）**：设定保留窗口（如最新的 $T − N$ 个版本），逻辑删除最老的 N 个版本(后面实验设置为前20%的backup )的 Recipe，但不做任何物理数据清除。
    
3. **GC 可达性模拟**：在逻辑删除后执行mark & sweep过程进行GC。
## 数据集
|           | 大小     | 数量  |
| --------- | ------ | --- |
| **Linux** | 199GiB | 270 |
| **Web**   | 277GiB | 102 |
| **Glibc** | 14GiB  | 100 |
## Metrics

| 名称                                 | 说明                                                    |
| ---------------------------------- | ----------------------------------------------------- |
| **Logical Backup Size**            | 过期备份的原始大小（未去重未压缩，重复引用计多次），即用户感知的删除量。                  |
| **Logical Reclaimable Size(LRS)**  | 去重语义下理论可物理删除的大小（仅属于过期版本、不被任何存活 Recipe 引用的独占块）         |
| **Pinned Size**                    | 依赖锁死大小（逻辑上归属过期版本，但被存活版本作为增量重建的 Base Chunk，因而无法物理回收）   |
| **Physical Reclaimable Size(PRS)** | 实际可物理回收的大小（`LRS - Pinned Size`），排除 Delta 依赖锁定后真正可删的）  |
| **LRPR = PRS / LRS**               | 空间回收效率（Physical-to-Logical Reclaim Ratio），越低说明依赖锁死越严重 |
预期 LBS ≥ LRS ≥ PRS，三者差距即为去重共享和 Delta 依赖分别造成的空间损耗。

**容器碎片化分布（Container Liveness Distribution）：**
对每个 4MB 容器，统计其中存活块（alive + pinned）占容器总字节的比例（Liveness Ratio），按 10% 粒度分桶输出直方图。关注点：

- 可完全回收容器数（Liveness = 0%）
- 严重碎片化容器数（Liveness < 30%）
# 实验结果
## 压缩率

|       | inline  | offline |
| ----- | ------- | ------- |
| Linux | 37.4756 | 65.6877 |
| Web   | 92.1916 | 341.066 |
| Glibc | 31.6662 | 45.284  |

## 垃圾回收

### **Linux**
删除前54个备份

|                                    |                                    |
| ---------------------------------- | ---------------------------------- |
| **Logical Backup Size**            | 12343132160 Bytes(**11771.33MiB**) |
| **Logical Reclaimable Size(LRS)**  | 328788630 Bytes (**313.557MiB**)   |
| **Pinned Size**                    | 152476570(**145.413 MiB**)         |
| **Physical Reclaimable Size(PRS)** | 176312060 Bytes(**168.144MiB**)    |
| **LRPR**                           | 53.6247%                           |
========== Container Liveness Distribution ==========
Total containers: 773
Fully reclaimable (0% alive):   0
Fully alive (100%):             669
----------------------------------------------------
  Liveness 0% (empty):    0
  Liveness 0-10%:         0
  Liveness 10-20%:        0
  Liveness 20-30%:        0
  Liveness 30-40%:        3
  Liveness 40-50%:        17
  Liveness 50-60%:        42
  Liveness 60-70%:        24
  Liveness 70-80%:        9
  Liveness 80-90%:        5
  Liveness 90-100%:       4
  Liveness 100% (full):   669
====================================================
### **Web**
删除前20个备份

|                                    |                                     |
| ---------------------------------- | ----------------------------------- |
| **Logical Backup Size**            | 58467983360 Bytes(**55,759.41MiB**) |
| **Logical Reclaimable Size(LRS)**  | 197834017 Bytes (**188.669MiB**)    |
| **Pinned Size**                    | 80614047(**76.8795 MB**)            |
| **Physical Reclaimable Size(PRS)** | 117219970 Bytes(**111.79MiB**)      |
| **LRPR**                           | 59.2517%                            |
========== Container Liveness Distribution ==========
Total containers: 209
Fully reclaimable (0% alive):   0
Fully alive (100%):             64
----------------------------------------------------
  Liveness 0% (empty):    0
  Liveness 0-10%:         0
  Liveness 10-20%:        1
  Liveness 20-30%:        3
  Liveness 30-40%:        2
  Liveness 40-50%:        4
  Liveness 50-60%:        13
  Liveness 60-70%:        8
  Liveness 70-80%:        9
  Liveness 80-90%:        48
  Liveness 90-100%:       57
  Liveness 100% (full):   64
====================================================
### **glibc**
删除前20个备份

|                                    |                                  |
| ---------------------------------- | -------------------------------- |
| **Logical Backup Size**            | 1424199680 Bytes(**1358.22MiB**) |
| **Logical Reclaimable Size(LRS)**  | 69564272 Bytes (**66.341MiB**)   |
| **Pinned Size**                    | 27519888 bytes (26.245 MB)       |
| **Physical Reclaimable Size(PRS)** | 42044384 Bytes(**40.09MiB**)     |
| **LRPR**                           | 60.4396%                         |
========== Container Liveness Distribution ==========
Total containers: 78
Fully reclaimable (0% alive):   0
Fully alive (100%):             51
----------------------------------------------------
  Liveness 0% (empty):    0
  Liveness 0-10%:         0
  Liveness 10-20%:        0
  Liveness 20-30%:        0
  Liveness 30-40%:        1
  Liveness 40-50%:        6
  Liveness 50-60%:        7
  Liveness 60-70%:        3
  Liveness 70-80%:        5
  Liveness 80-90%:        3
  Liveness 90-100%:       2
  Liveness 100% (full):   51
====================================================
## 分析

### 产生**Pinned Size** 的一些Cases

1. 这是最直观的情况：MDelta 的 CutGreedy 在 Offline 阶段选择了一个老版本的块作为 base，新版本被编码为相对于它的 delta。删除老版本后，这个 base 就被钉住了。![[Case1.png]]
2. MDelta 允许 "delta of delta"，一条链可以跨越数十个版本——链上所有节点都成为不可回收的 zombie。![[Case2.png]]
3. **MDelta 的存储优化越激进，制造的跨版本依赖越多，GC 的回收率就越低。**![[Case3.png]]
4. 只要容器里混有一个被 Pin 的老 base，整个容器就无法释放。MDelta 的全局优化倾向于选择"好的"base chunk，这些高质量 base 往往扇出度很高（被大量 delta 引用），导致它们几乎不可能被删除。![[Case4.png]

# 单层增量压缩下的实验结果

## Linux
|                                    |                                    |
| ---------------------------------- | ---------------------------------- |
| **Logical Backup Size**            | 12343132160 Bytes(**11771.33MiB**) |
| **Logical Reclaimable Size(LRS)**  | 328788630 Bytes (**450.326MiB**)   |
| **Pinned Size**                    | 152476570(**135.348 MiB**)         |
| **Physical Reclaimable Size(PRS)** | 176312060 Bytes(**314.978 MiB**)   |
| **LRPR**                           | 69.9444%                           |
========== Container Liveness Distribution ==========
Total containers: 1355
Fully reclaimable (0% alive):   0
Fully alive (100%):             1234

----------------------------------------------------
  Liveness 0% (empty):    0
  Liveness 0-10%:         12
  Liveness 10-20%:        21
  Liveness 20-30%:        16
  Liveness 30-40%:        24
  Liveness 40-50%:        26
  Liveness 50-60%:        10
  Liveness 60-70%:        6
  Liveness 70-80%:        3
  Liveness 80-90%:        2
  Liveness 90-100%:       1
  Liveness 100% (full):   1234
====================================================

Severely fragmented containers (<30% alive): 49
Fragmentation ratio: 3.61624%
## Web
|                                    |                                     |
| ---------------------------------- | ----------------------------------- |
| **Logical Backup Size**            | 58467983360 Bytes(**55,759.41MiB**) |
| **Logical Reclaimable Size(LRS)**  | 197834017 Bytes (**619.875MiB**)    |
| **Pinned Size**                    | 80614047(**68.6069 MiB**)           |
| **Physical Reclaimable Size(PRS)** | 117219970 Bytes(**551.269 MiB**)    |
| **LRPR**                           | 88.9321%                            |
========== Container Liveness Distribution ==========
Total containers: 770
Fully reclaimable (0% alive):   0
Fully alive (100%):             543

-----------------------------------------------------------------
  Liveness 0% (empty):    0
  Liveness 0-10%:         52
  Liveness 10-20%:        59
  Liveness 20-30%:        24
  Liveness 30-40%:        10
  Liveness 40-50%:        3
  Liveness 50-60%:        2
  Liveness 60-70%:        3
  Liveness 70-80%:        7
  Liveness 80-90%:        57
  Liveness 90-100%:       10
  Liveness 100% (full):   543
====================================================

Severely fragmented containers (<30% alive): 135
Fragmentation ratio: 17.5325%
## glibc
|                                    |                                  |
| ---------------------------------- | -------------------------------- |
| **Logical Backup Size**            | 1424199680 Bytes(**1358.22MiB**) |
| **Logical Reclaimable Size(LRS)**  | 69564272 Bytes (**80.0733MiB**)  |
| **Pinned Size**                    | 27519888 bytes (24.6312 MB)      |
| **Physical Reclaimable Size(PRS)** | 42044384 Bytes(**55.4421 MiB**)  |
| **LRPR**                           | 69.2392%                         |

========== Container Liveness Distribution ==========
Total containers: 111
Fully reclaimable (0% alive):   0
Fully alive (100%):             85

----------------------------------------------------
  Liveness 0% (empty):    0
  Liveness 0-10%:         0
  Liveness 10-20%:        0
  Liveness 20-30%:        4
  Liveness 30-40%:        7
  Liveness 40-50%:        6
  Liveness 50-60%:        5
  Liveness 60-70%:        1
  Liveness 70-80%:        1
  Liveness 80-90%:        1
  Liveness 90-100%:       1
  Liveness 100% (full):   85
====================================================

Severely fragmented containers (<30% alive): 4
Fragmentation ratio: 3.6036%