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

# 实验组合列表：数据集 分块方法 在线方法 离线方法 是否恢复
experiments=(
  "glibc 1 3 -1 1"
  "glibc 1 3 0 1"
  "linux 1 3 -1 1"
  "linux 1 3 0 1"
  "WEB-3 1 3 -1 1"
  "WEB-3 1 3 0 1"
  "WindowsLog 1 3 -1 1"
  "WindowsLog 1 3 0 1"
  # ...
)

for exp in "${experiments[@]}"; do
  # 清空 restoreFile 文件夹内容
  rm -rf restoreFile/*

  read -r dataset chunking online offline restore <<< "$exp"
  read -r path num <<< "${datasets[$dataset]}"
  offline_arg=""
  outname=""
  if [[ $offline -ge 0 ]]; then
    offline_arg="-o $offline"
    outname="offline${offline}_"
  fi
  ./DFree -i "$path" -c "$chunking" -m "$online" -n "$num" $offline_arg -R "$restore" > "${outname}C${chunking}_M${online}_${dataset}_R${restore}.txt"
  echo "完成：$dataset 分块$chunking 在线$online 离线$offline 恢复$restore"
done

echo "全部实验完成"