cd bin
# 设置为 "true" 来为所有测试添加 -R 1 标志，设置为 "false" 或其他任何值则不添加。
USE_R_FLAG="true"
# ----------------

# 根据开关确定 R 标志参数
R_FLAG_PARAM=""
if [[ "$USE_R_FLAG" == "true" ]]; then
  R_FLAG_PARAM="-R 1"
fi

declare -A datasets
datasets=(
  ["automake"]="/home/public/Dataset/automake_tarballs 100"
  ["bash"]="/mnt/dataset2/bash_tarballs 44"
  ["coreutils"]="/mnt/dataset2/coreutils_tarballs 28"
  ["fdisk"]="/mnt/dataset2/fdisk_tarballs 22"
  ["glibc"]="/home/public/Dataset/glibc_tarballs 100"
  ["smalltalk"]="/mnt/dataset2/smalltalk_tarballs 40"
  ["gcc"]="/mnt/dataset2/GNU_GCC/gcc-packed/tar 117"
  ["chromium"]="/mnt/dataset2/chromium 107"
  ["linux"]="/home/public/Dataset/linux 270"
  ["cassandra"]="/mnt/dataset2/cassandra 97"
  ["vmdk"]="/mnt/dataset2/vmdk 8"
  ["WEB"]="/home/public/Dataset/WEB 20"
  ["WindowsLog"]="/home/public/Dataset/WindowsLog 1"
  ["ThunderbirdLog"]="/mnt/dataset2/ThunderbirdLog 1"
  ["Wiki"]="/mnt/dataset2/wiki2025 7"
)

declare -A methods
methods=(
  ["Odess"]="-c 1 -m 3"
  # ["TreeCut"]="-c 1 -m 12"
  # ["Greedy"]="-c 1 -m 13"
  # ["TreeCutLayer"]="-c 1 -m 15"
  #  ["TreeCache"]="-c 1 -m 16"
  # ["TreeCache2"]="-c 1 -m 17"
  # ["SubTree"]="-c 1 -m 18"
  # ["OdessMiBL"]="-c 1 -m 6"
  # ["OdessMiBL2"]="-c 1 -m 8"
  # ["OdessMiBL3"]="-c 1 -m 9"
  # ["OdessMiLess4"]="-c 1 -m 11"
  # ["OdessMiLog2"]="-c 1 -m 10"
  # ["AllGreedy"]="-c 1 -m 14"
  # ["offlineAllGreedy"]="-c 1 -m 3 -o 0"
  # ["offlineTreeCut"]="-c 1 -m 3 -o 1"
  # ["offlineTreeCutLayer"]="-c 1 -m 3 -o 2"
  # ["offlineTreeCache"]="-c 1 -m 3 -o 3"

  # design1
  # ["Design1"]="-c 1 -m 3 -o 1"
  # design2
  ["Design2"]="-c 1 -m 3 -o 5"
  # ["offlineTreeCutLayer"]="-c 1 -m 3 -o 2"
  # ["offlineTreeIngnore"]="-c 1 -m 3 -o 5"
  # design3
  ["Design3"]="-c 1 -m 3 -o 6"
  # ["offlineTreeFeatureLru"]="-c 1 -m 3 -o 6"
  ["offlineTreeFeature"]="-c 1 -m 3 -o 4"
)

selected_datasets=(
  # "automake"
  # "bash"
  # "coreutils"
  "WindowsLog"
  # "fdisk"
  # "glibc"
  # "smalltalk"
  # "gcc"
  # "chromium"
  # "linux"
  # "cassandra"
  # "vmdk"
  # "WEB"
  # "ThunderbirdLog"
  # "Wiki"
)
execution_order=(
  # "Design3"
  # "offlineTreeFeature"
  "Design2"
  # "Design1"
)


for method_name in "${execution_order[@]}"; do

if [[ -z "${methods[$method_name]}" ]]; then
    echo "Warning: Method '$method_name' is not defined in 'methods' array, skipping."
    continue
  fi
  method_params="${methods[$method_name]}"
  echo "Running method: $method_name"
  
  for dataset in "${selected_datasets[@]}"; do
    if [[ -n "${datasets[$dataset]}" ]]; then
      read -r path num <<< "${datasets[$dataset]}"
      echo "Processing dataset: $dataset"
      
      # sudo rm -f Containers/*
      # sudo rm -f OfflineContainers/*
      # sudo rm -f restoreFile/*
      # sync
      # echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
      if [[ ("$method_name" == "offlineAllGreedy" || "$method_name" == "AllGreedy") && "$dataset" == "WEB" ]]; then
        echo "Applying special rule for $method_name on WEB: changing num to 3"
        num=3
      fi
      
      # 在执行命令中加入 $R_FLAG_PARAM
      ./DFree -i "$path" $method_params $R_FLAG_PARAM -n "$num" > "${method_name}_${dataset}.txt"

      #  # --- perf start ---
      # PERF_DATA_FILE="perf_${method_name}_${dataset}.data"
      
      # echo "Recording performance data to $PERF_DATA_FILE"
      
      # # 使用 -o 选项指定输出文件名
      #       sudo perf record -o "$PERF_DATA_FILE" -F 99 -g -- ./DFree -i "$path" $method_params $R_FLAG_PARAM -n "$num" > "perf_output_${method_name}_${dataset}.txt"
      # # --- perf end---
      
      echo "Completed $method_name on $dataset"
    else
      echo "Error: Dataset $dataset not defined"
    fi
  done
  
  echo "Method $method_name completed on all selected datasets"
  echo "----------------------------------------"
done

echo "All tests completed"
