cd bin

declare -A datasets
datasets=(
#   ["automake"]="/mnt/dataset2/automake_tarballs 100"
#   ["bash"]="/mnt/dataset2/bash_tarballs 44"
#   ["coreutils"]="/mnt/dataset2/coreutils_tarballs 28"
#   ["fdisk"]="/mnt/dataset2/fdisk_tarballs 22"
  ["glibc"]="/mnt/dataset2/glibc_tarballs 100"
#   ["smalltalk"]="/mnt/dataset2/smalltalk_tarballs 40"
#   ["gcc"]="/mnt/dataset2/GNU_GCC/gcc-packed/tar 117"
#   ["chromium"]="/mnt/dataset2/chromium 107"
#   ["linux-100"]="/mnt/dataset2/linux 100"
  ["linux"]="/mnt/dataset2/linux 270"
#   ["cassandra"]="/mnt/dataset2/cassandra 97"
#   ["vmdk"]="/mnt/dataset2/vmdk 8"
  ["WEB"]="/mnt/dataset2/WEB 20"
  ["WEB-3"]="/mnt/dataset2/WEB 3"
  ["WindowsLog"]="/mnt/dataset2/WindowsLog 1"
#   ["ThunderbirdLog"]="/mnt/dataset2/ThunderbirdLog 1"
#   ["Wiki"]="/mnt/dataset2/wiki2025 7"
)

chunking_methods=(1)           # 分块方法列表
online_methods=(3 12)          # 在线方法编号列表
offline_methods=(-1 0 1)       # -1表示不做离线，其他为离线方法编号
restore_options=(0 1)          # 是否恢复

for dataset in "${!datasets[@]}"; do
  read -r path num <<< "${datasets[$dataset]}"
  for chunking in "${chunking_methods[@]}"; do
    for online in "${online_methods[@]}"; do
      for offline in "${offline_methods[@]}"; do
        for restore in "${restore_options[@]}"; do
          offline_arg=""
          outname=""
          if [[ $offline -ge 0 ]]; then
            offline_arg="-o $offline"
            outname="offline${offline}_"
          fi
          ./DFree -i "$path" -c "$chunking" -m "$online" -n "$num" $offline_arg -R "$restore" > "${outname}C${chunking}_M${online}_${dataset}_R${restore}.txt"
          echo "完成：$dataset 分块$chunking 在线$online 离线$offline 恢复$restore"
        done
      done
    done
  done
done

echo "全部实验完成"