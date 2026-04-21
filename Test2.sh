#!/usr/bin/env bash

cd bin

declare -A datasets
datasets=(
  # ["automake"]="/mnt/dataset2/automake_tarballs 100"
  # ["bash"]="/mnt/dataset2/bash_tarballs 44"
  # ["coreutils"]="/mnt/dataset2/coreutils_tarballs 28"
  # ["fdisk"]="/mnt/dataset2/fdisk_tarballs 22"
  ["glibc"]="/home/yp/datasets/glibc_tarballs 100"
  # ["smalltalk"]="/mnt/dataset2/smalltalk_tarballs 40"
  # ["gcc"]="/mnt/dataset2/GNU_GCC/gcc-packed/tar 117"
  # ["chromium"]="/mnt/dataset2/chromium 107"
  # ["linux-100"]="/mnt/dataset2/linux 100"
  # ["linux"]="/home/public/Dataset/linux 270"
  # ["cassandra"]="/mnt/dataset2/cassandra 97"
  # ["vmdk"]="/mnt/dataset2/vmdk 8"
  # ["WEB"]="/mnt/dataset2/WEB 20"
  # ["WEB-3"]="/home/public/Dataset/WEB 3"
  # ["WindowsLog"]="/home/public/Dataset/WindowsLog 1"
  # ["ThunderbirdLog"]="/mnt/dataset2/ThunderbirdLog 1"
  # ["Wiki"]="/mnt/dataset2/wiki2025 7"
  # ["docker"]="/home/public/Dataset/docker 130"
)

# 固定分块方法为单一值
chunking=1

# 只用修改这里
online_methods=(3)          # 在线方法编号列表
offline_methods=(10 11)       # -1表示不做离线，其他为离线方法编号
restore_options=(1)         # 是否恢复  0,1
threshold=64              # 新增：阈值参数，可根据需要修改

for dataset in "${!datasets[@]}"; do
  read -r path num <<< "${datasets[$dataset]}"
  for online in "${online_methods[@]}"; do
    for offline in "${offline_methods[@]}"; do
      for restore in "${restore_options[@]}"; do
        # 清空 restoreFile 文件夹内容
        rm -rf restoreFile/*

        # 输出实验开始时间及当前实验信息
        experiment_desc="dataset=${dataset} path=${path} n=${num} C${chunking} M${online} offline${offline} R${restore} T${threshold}"
        echo "实验开始时间：$(date) - 正在执行：${experiment_desc}"

        offline_arg=""
        outname=""
        threshold_arg=""
        if [[ $offline -ge 0 ]]; then
          offline_arg="-o $offline"
          outname="offline${offline}_"
          threshold_arg="-T $threshold"
        fi

        sync
        if ! echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; then
          echo "清理页缓存失败，请确认当前用户有 sudo 权限，或先手动执行一次: echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null" >&2
          exit 1
        fi

        ./DFree -i "$path" -c "$chunking" -m "$online" -n "$num" $offline_arg $threshold_arg -R "$restore" > "${outname}C${chunking}_M${online}_${dataset}_R${restore}_T${threshold}.txt"
        echo "完成：$dataset 分块$chunking 在线$online 离线$offline 恢复$restore 阈值$threshold"
      done
    done
  done
done

echo "全部实验完成"
