cd bin

declare -A datasets
datasets=(
  # ["automake"]="/mnt/dataset2/automake_tarballs 100"
  # ["bash"]="/mnt/dataset2/bash_tarballs 44"
  # ["coreutils"]="/mnt/dataset2/coreutils_tarballs 28"
  # ["fdisk"]="/mnt/dataset2/fdisk_tarballs 22"
  # ["glibc"]="/home/public/Dataset/glibc_tarballs/glibc_tarballs 100"
  # ["smalltalk"]="/mnt/dataset2/smalltalk_tarballs 40"
  # ["gcc"]="/mnt/dataset2/GNU_GCC/gcc-packed/tar 117"
  # ["chromium"]="/mnt/dataset2/chromium 107"
  # ["linux-100"]="/mnt/dataset2/linux 100"
  ["linux"]="/home/public/Dataset/linux 270"
  # ["cassandra"]="/mnt/dataset2/cassandra 97"
  # ["vmdk"]="/mnt/dataset2/vmdk 8"
  # ["WEB"]="/mnt/dataset2/WEB 20"
  # ["WEB-3"]="/home/public/Dataset/WEB 3"
  # ["WindowsLog"]="/home/public/Dataset/WindowsLog 1"
  # ["ThunderbirdLog"]="/mnt/dataset2/ThunderbirdLog 1"
  # ["Wiki"]="/mnt/dataset2/wiki2025 7"
)

# 固定分块方法为单一值
chunking=1

# 只用修改这里
online_methods=(3)          # 在线方法编号列表
offline_methods=(6)       # -1表示不做离线，其他为离线方法编号
restore_options=(0)          # 是否恢复  0,1

for dataset in "${!datasets[@]}"; do
  read -r path num <<< "${datasets[$dataset]}"
  for online in "${online_methods[@]}"; do
    for offline in "${offline_methods[@]}"; do
      for restore in "${restore_options[@]}"; do
        # 清空 restoreFile 文件夹内容
        rm -rf restoreFile/*

        # 输出实验开始时间及当前实验信息
        experiment_desc="dataset=${dataset} path=${path} n=${num} C${chunking} M${online} offline${offline} R${restore}"
        echo "实验开始时间：$(date) - 正在执行：${experiment_desc}"

        offline_arg=""
        outname=""
        if [[ $offline -ge 0 ]]; then
          offline_arg="-o $offline"
          outname="offline${offline}_"
        fi

        sudo echo 3 > /proc/sys/vm/drop_caches

        ./DFree -i "$path" -c "$chunking" -m "$online" -n "$num" $offline_arg -R "$restore" > "${outname}C${chunking}_M${online}_${dataset}_R${restore}.txt"
        echo "完成：$dataset 分块$chunking 在线$online 离线$offline 恢复$restore"
      done
    done
  done
done

echo "全部实验完成"