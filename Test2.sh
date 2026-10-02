#!/usr/bin/env bash

cd bin

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
  # ["linux"]="/home/public/Dataset/linux 270"
  # ["cassandra"]="/mnt/dataset2/cassandra 97"
  # ["vmdk"]="/mnt/dataset2/vmdk 8"
  # ["WEB"]="/home/public/Dataset/WEB 102"
  # ["WEB-3"]="/home/public/Dataset/WEB 3"
  # ["WindowsLog"]="/home/public/Dataset/logs 112"
  # ["ThunderbirdLog"]="/mnt/dataset2/ThunderbirdLog 1"
  # ["Wiki"]="/mnt/dataset2/wiki2025 7"
  # ["docker"]="/home/public/Dataset/docker_gitlab 100"
)

# 固定分块方法为单一值
chunking=1

# 只用修改这里
online_methods=(3)          # 在线方法编号列表
offline_methods=(10)       # -1表示不做离线，其他为离线方法编号
restore_options=(1)         # 是否恢复  0,1
thresholds=(64)             # 阈值参数列表，可填多个值如 (64 128 256)
batch_sizes=(10)            # 每批版本数；仅离线实验生效

for dataset in "${!datasets[@]}"; do
  read -r path num <<< "${datasets[$dataset]}"
  for online in "${online_methods[@]}"; do
    for offline in "${offline_methods[@]}"; do
      for restore in "${restore_options[@]}"; do
        # 清空 restoreFile 文件夹内容
        rm -rf restoreFile/*

        for th in "${thresholds[@]}"; do
          for batch_size in "${batch_sizes[@]}"; do
            # 输出实验开始时间及当前实验信息
            experiment_desc="dataset=${dataset} path=${path} n=${num} C${chunking} M${online} offline${offline} R${restore} T${th} B${batch_size}"
            echo "实验开始时间：$(date) - 正在执行：${experiment_desc}"

            offline_args=()
            outname=""
            if [[ $offline -ge 0 ]]; then
              offline_args=(-o "$offline" -T "$th" -B "$batch_size")
              outname="offline${offline}_"
            fi

            sync
            if ! echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; then
              echo "清理页缓存失败，请确认当前用户有 sudo 权限，或先手动执行一次: echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null" >&2
              exit 1
            fi

            logfile="${outname}C${chunking}_M${online}_${dataset}_R${restore}_T${th}_B${batch_size}.txt"
            if ! ./DFree -i "$path" -c "$chunking" -m "$online" -n "$num" \
              "${offline_args[@]}" -R "$restore" > "$logfile"; then
              echo "实验失败，请检查日志：$logfile" >&2
              exit 1
            fi
            echo "完成：$dataset 分块$chunking 在线$online 离线$offline 恢复$restore 阈值$th 批大小$batch_size"
          done
        done
      done
    done
  done
done

echo "全部实验完成"
