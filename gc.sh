cd bin
# ./DFree -i /mnt/dataset2/glibc_tarballs/glibc_tarballs/ -c 1 -m 3 -o -1 -n 100 -G 1 -D 20 > glibc_20_inline.txt
# ./DFree -i /mnt/dataset2/linux -c 1 -m 3 -o -1 -n 270 -G 1 -D 54 > linux_54_inline.txt
# ./DFree -i /mnt/dataset2/WEB -c 1 -m 3 -o -1 -n 102 -G 1 -D 20 > web_20_inline.txt
./DFree -i /mnt/dataset2/cassandra -c 1 -m 3 -o -1 -n 97 -G 1 -D 20 > cassandra_20_inline.txt
./DFree -i /mnt/dataset2/vmdk -c 1 -m 3 -o -1 -n 10 -G 1 -D 2 > vmdk_2_inline.txt
# ./DFree -i /mnt/dataset2/glibc_tarballs/glibc_tarballs/ -c 1 -m 3 -o 6 -n 100 -G 1 -D 20 > glibc_20_offline.txt
# ./DFree -i /mnt/dataset2/linux -c 1 -m 3 -o 6 -n 270 -G 1 -D 54 > linux_54_offline.txt
# ./DFree -i /mnt/dataset2/WEB -c 1 -m 3 -o 6 -n 102 -G 1 -D 20 > web_20_offline.txt
./DFree -i /mnt/dataset2/cassandra -c 1 -m 3 -o 6 -n 97 -G 1 -D 20 > cassandra_20_offline.txt
./DFree -i /mnt/dataset2/vmdk -c 1 -m 3 -o 6 -n 10 -G 1 -D 2 > vmdk_2_offline.txt