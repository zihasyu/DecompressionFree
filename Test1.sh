cd bin

# 数据集路径和版本数
declare -A datasets
datasets=(
  # ["automake"]="/mnt/dataset2/automake_tarballs 100"
  # ["bash"]="/mnt/dataset2/bash_tarballs 44"
  # ["coreutils"]="/mnt/dataset2/coreutils_tarballs 28"
  # ["fdisk"]="/mnt/dataset2/fdisk_tarballs 22"
  ["glibc"]="/home/public/Dataset/glibc_tarballs/glibc_tarballs 100"
  # ["smalltalk"]="/mnt/dataset2/smalltalk_tarballs 40"
  # ["gcc"]="/mnt/dataset2/GNU_GCC/gcc-packed/tar 117"
  # ["chromium"]="/mnt/dataset2/chromium 107"
  # ["linux-100"]="/mnt/dataset2/linux 100"
  ["linux"]="/home/public/Dataset/linux 270"
  # ["cassandra"]="/mnt/dataset2/cassandra 97"
  # ["vmdk"]="/mnt/dataset2/vmdk 8"
  # ["WEB"]="/mnt/dataset2/WEB 20"
  ["WEB-3"]="/home/public/Dataset/WEB 3"
  ["WindowsLog"]="/home/public/Dataset/WindowsLog 1"
  # ["ThunderbirdLog"]="/mnt/dataset2/ThunderbirdLog 1"
  # ["Wiki"]="/mnt/dataset2/wiki2025 7"
)

# 所有实验使用相同的分块方法
chunking=1

# 增加/修改方法组合时只改这里
# 实验方法组合（只写方法相关，不包含分块）
# 格式：在线 离线 恢复
experiments=(
  "19 -1 0"
  "20 -1 0"
)

# 生成数据集列表（由 datasets 自动提供）
dataset_names=()
for k in "${!datasets[@]}"; do
  dataset_names+=("$k")
done

# 构建按方法组交错运行的数据顺序：对每个方法组合，依次对所有数据集运行
runs=()
for exp in "${experiments[@]}"; do
  for dataset in "${dataset_names[@]}"; do
    runs+=("$dataset $exp")
  done
done

# 运行所有任务（相邻两项不会处理同一数据集，除非只有一个数据集）
for task in "${runs[@]}"; do
  # 清空 restoreFile 文件夹内容
  rm -rf restoreFile/*

  # 先解析任务信息
  read -r dataset online offline restore <<< "$task"
  read -r path num <<< "${datasets[$dataset]}"

  # 输出实验开始时间及当前实验信息
  experiment_desc="dataset=${dataset} path=${path} n=${num} C${chunking} M${online} offline${offline} R${restore}"
  echo "实验开始时间：$(date) - 正在执行：${experiment_desc}"

  offline_arg=""
  outname=""
  if [[ $offline -ge 0 ]]; then
    offline_arg="-o $offline"
    outname="offline${offline}_"
  fi

  sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

  ./DFree -i "$path" -c "$chunking" -m "$online" -n "$num" $offline_arg -R "$restore" > "${outname}C${chunking}_M${online}_${dataset}_R${restore}.txt"
  echo "完成：$dataset 分块$chunking 在线$online 离线$offline 恢复$restore"
done

echo "全部实验完成"
