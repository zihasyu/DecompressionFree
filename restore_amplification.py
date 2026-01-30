import re

input_file = 'bin/C1_M3_linux_R1_T64.txt'
output_file = 'restore_amplification/linux_odess.txt'

write_amps = []

with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        match = re.search(r'Version read amplification:\s*([\d\.]+)', line)
        if match:
            write_amps.append(match.group(1))

with open(output_file, 'w', encoding='utf-8') as f:
    for amp in write_amps:
        f.write(f"{amp}\n")

print(f"已提取{len(write_amps)}个写放大比到 {output_file}")