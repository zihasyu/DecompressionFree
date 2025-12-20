cd bin

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
  # ["Odess"]="-c 1 -m 3"
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
      ["offlineTreeCut"]="-c 1 -m 3 -o 0"
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
  "linux"
  # "cassandra"
  # "vmdk"
  "WEB"
  # "ThunderbirdLog"
  # "Wiki"
)

for method_name in "${!methods[@]}"; do
  method_params="${methods[$method_name]}"
  echo "Running method: $method_name"
  
  for dataset in "${selected_datasets[@]}"; do
    if [[ -n "${datasets[$dataset]}" ]]; then
      read -r path num <<< "${datasets[$dataset]}"
      echo "Processing dataset: $dataset"
      
    #   sudo rm -f Containers/*
    #   sudo echo 3 > /proc/sys/vm/drop_caches
      
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