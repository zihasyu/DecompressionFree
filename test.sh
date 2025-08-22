cd bin
run_method(){
  local path=$1
  local name=$2
  local num=$3


# # sudo rm Containers/*
# # sudo echo 3 > /proc/sys/vm/drop_caches
# ./DFree -i $path -c 1 -m 6 -n $num  >OdessMiBL$name.txt
# # sudo rm Containers/*

# # sudo echo 3 > /proc/sys/vm/drop_caches
# ./DFree -i $path -c 1 -m 8 -n $num  >OdessMiBL2$name.txt
# # sudo rm Containers/*
# # sudo echo 3 > /proc/sys/vm/drop_caches
# ./DFree -i $path -c 1 -m 9 -n $num  >OdessMiBL3$name.txt
# sudo rm Containers/*
# sudo echo 3 > /proc/sys/vm/drop_caches
# ./DFree -i $path -c 1 -m 11 -n $num  >OdessMiLess4$name.txt

# ./DFree -i $path -c 1 -m 10 -n $num  >OdessMiLog2$name.txt
# ./DFree -i $path -c 1 -m 3 -n $num  >Odess$name.txt
# ./DFree -i $path -c 4 -m 3 -n $num  >OdessFile$name.txt
# ./DFree -i $path -c 4 -m 6 -n $num  >OdessFileMiBL$name.txt
# ./DFree -i $path -c 4 -m 10 -n $num  >OdessFileMiLog2$name.txt


# ./DFree -i $path -c 1 -m 3 -n $num  >Odess$name.txt
# ./DFree -i $path -c 1 -m 12 -n $num  >TreeCut$name.txt
# ./DFree -i $path -c 1 -m 13 -n $num  >TreeGreedy$name.txt
# ./DFree -i $path -c 1 -m 14 -n $num  >AllGreedy$name.txt
# ./DFree -i $path -c 1 -m 15 -n $num  >TreeCutLayer$name.txt
./DFree -i $path -c 1 -m 16 -n $num  >TreeCacheHot$name.txt
}


# run_method /mnt/dataset2/automake_tarballs _automake 100
# run_method /mnt/dataset2/bash_tarballs _bash 44
# run_method /mnt/dataset2/coreutils_tarballs _coreutils 28
# run_method /mnt/dataset2/fdisk_tarballs _fdisk 22
# run_method /mnt/dataset2/glibc_tarballs _glibc 100
# run_method /mnt/dataset2/smalltalk_tarballs _smalltalk 40
# run_method /mnt/dataset2/GNU_GCC/gcc-packed/tar _gcc 117
# run_method /mnt/dataset2/chromium _chromium 107
run_method /mnt/dataset2/linux _linux 270


# run_method /mnt/dataset2/WEB _WEB 102
# run_method /mnt/dataset2/Windows Windows 738
# run_method /mnt/dataset2/Android _Android 36
# run_method /mnt/dataset2/ThunderbirdTar _Thunderbird 240

# run_method /mnt/dataset2/linux _linux 200
