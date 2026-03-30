#!/usr/bin/env bash

set -u

SCRIPT_DIR=$(dirname "$(realpath "$0")")
cd "$SCRIPT_DIR/bin"

if [[ ${EUID:-$(id -u)} -eq 0 ]]; then
  echo "请直接用普通用户运行这个脚本，不要再用 sudo bash Test2.sh。" >&2
  exit 1
fi

for required_dir in . restoreFile; do
  if [[ ! -w "$required_dir" ]]; then
    echo "目录 $PWD/$required_dir 当前不可写，请先修复权限后再运行。" >&2
    echo "例如：sudo chown -R $(id -un):$(id -gn) $PWD/$required_dir" >&2
    exit 1
  fi
done

if ! sudo -v; then
  echo "无法获取 sudo 权限，drop_caches 步骤需要 sudo。" >&2
  exit 1
fi

while true; do
  sudo -n true
  sleep 60
done 2>/dev/null &
SUDO_KEEPALIVE_PID=$!
trap 'kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true' EXIT

declare -A datasets
datasets=(
  # ["automake"]="/mnt/dataset2/automake_tarballs 100"
  # ["bash"]="/mnt/dataset2/bash_tarballs 44"
  # ["coreutils"]="/mnt/dataset2/coreutils_tarballs 28"
  # ["fdisk"]="/mnt/dataset2/fdisk_tarballs 22"
  ["glibc"]="/home/public/Dataset/glibc_tarballs/glibc_tarballs 100"  #100
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
offline_methods=(12)       # -1表示不做离线，其他为离线方法编号
restore_options=(1)         # 是否恢复  0,1
threshold=64              # 新增：阈值参数，可根据需要修改
retention_backups=20      # 新增：保留最近多少个备份，-1 表示全部保留

for dataset in "${!datasets[@]}"; do
  read -r path num <<< "${datasets[$dataset]}"
  for online in "${online_methods[@]}"; do
    for offline in "${offline_methods[@]}"; do
      for restore in "${restore_options[@]}"; do
        # 清空 restoreFile 文件夹内容
        rm -rf restoreFile/*

        # 输出实验开始时间及当前实验信息
        offline_arg=""
        outname=""
        threshold_arg=""
        retention_arg=""
        retention_tag=""
        if [[ $offline -ge 0 ]]; then
          offline_arg="-o $offline"
          outname="offline${offline}_"
          threshold_arg="-T $threshold"
        fi
        if [[ $retention_backups -ge 0 ]]; then
          retention_arg="-k $retention_backups"
          retention_tag="_K${retention_backups}"
        fi

        experiment_desc="dataset=${dataset} path=${path} n=${num} C${chunking} M${online} offline${offline} R${restore} T${threshold} K${retention_backups}"
        echo "实验开始时间：$(date) - 正在执行：${experiment_desc}"

        sync
        if ! echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null; then
          echo "清理页缓存失败，请确认当前用户有 sudo 权限。" >&2
          exit 1
        fi

        logfile="${outname}C${chunking}_M${online}_${dataset}_R${restore}_T${threshold}${retention_tag}.txt"
        ./DFree -i "$path" -c "$chunking" -m "$online" -n "$num" $offline_arg $threshold_arg $retention_arg -R "$restore" > "$logfile" 2>&1
        status=$?
        if [[ $status -ne 0 ]]; then
          echo "实验失败：${experiment_desc}" >&2
          echo "错误日志：$PWD/$logfile" >&2
          exit $status
        fi
        echo "完成：$dataset 分块$chunking 在线$online 离线$offline 恢复$restore 阈值$threshold 保留$retention_backups"
      done
    done
  done
done

echo "全部实验完成"
