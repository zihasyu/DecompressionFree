import re

input_file = 'bin/C1_M14_linux_R1_T64.txt'
output_file = 'restore_time/linux_allGreedy.txt'

restore_times = []

with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        match = re.search(r'Restore time:\s*([\d\.]+)', line)
        if match:
            restore_times.append(match.group(1))

with open(output_file, 'w', encoding='utf-8') as f:
    for t in restore_times:
        f.write(f"{t}\n")

print(f"已提取{len(restore_times)}个恢复时间到 {output_file}")