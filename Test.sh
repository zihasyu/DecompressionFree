cd bin

declare -A datasets
datasets=(
  ["automake"]="/mnt/dataset2/automake_tarballs 100"
  ["bash"]="/mnt/dataset2/bash_tarballs 44"
  ["coreutils"]="/mnt/dataset2/coreutils_tarballs 28"
  ["fdisk"]="/mnt/dataset2/fdisk_tarballs 22"
  ["glibc"]="/mnt/dataset2/glibc_tarballs 100"
  ["smalltalk"]="/mnt/dataset2/smalltalk_tarballs 40"
  ["gcc"]="/mnt/dataset2/GNU_GCC/gcc-packed/tar 117"
  ["chromium"]="/mnt/dataset2/chromium 107"
  ["linux"]="/mnt/dataset2/linux 270"
)

declare -A methods
methods=(
  ["Odess"]="-c 1 -m 3"
  ["TreeCut"]="-c 1 -m 12"
  ["AllGreedy"]="-c 1 -m 13"
  # 可以添加或注释其他方法
  # ["OdessMiBL"]="-c 1 -m 6"
  # ["OdessMiBL2"]="-c 1 -m 8"
  # ["OdessMiBL3"]="-c 1 -m 9"
  # ["OdessMiLess4"]="-c 1 -m 11"
  # ["OdessMiLog2"]="-c 1 -m 10"
)

# 定义要运行的数据集列表（可以修改此数组来选择要跑的数据集）
selected_datasets=(
  "automake"
  "bash"
  "coreutils"
  "fdisk"
  "glibc"
  # "smalltalk"
  # "gcc"
  # "chromium"
  # "linux"
)

# 先遍历方法，每个方法跑完所有数据集
for method_name in "${!methods[@]}"; do
  method_params="${methods[$method_name]}"
  echo "Running method: $method_name"
  
  # 对每个选中的数据集执行当前方法
  for dataset in "${selected_datasets[@]}"; do
    if [[ -n "${datasets[$dataset]}" ]]; then
      read -r path num <<< "${datasets[$dataset]}"
      echo "Processing dataset: $dataset"
      
      # 清理缓存，确保公平的缓冲区状态
    #   sudo rm -f Containers/*
    #   sudo echo 3 > /proc/sys/vm/drop_caches
      
      # 执行测试并输出到对应文件
      ./DFree -i "$path" $method_params -n "$num" > "${method_name}_${dataset}.txt"
      
      echo "Completed $method_name on $dataset"
    else
      echo "Error: Dataset $dataset not defined"
    fi
  done
  
  echo "Method $method_name completed on all selected datasets"
  echo "----------------------------------------"
done

echo "All tests completed"