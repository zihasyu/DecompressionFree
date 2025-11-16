# python
import re,sys

path = r'/home/cluster/YP/DecompressionFree/bin/Odess_linux.txt'
output_path = r'/home/cluster/YP/DecompressionFree/Odess_linux.csv'

ver = None
pat_ver = re.compile(r'^\s*Version:\s*(\d+)\s*$')
pat_dce = re.compile(r'^\s*DCE:\s*([0-9.eE+-]+)\s*$')

with open(path, 'r', encoding='utf-8', errors='ignore') as f, \
     open(output_path, 'w', encoding='utf-8', newline='') as out:
    out.write("Version,DCE\n")
    for line in f:
        m = pat_ver.match(line)
        if m:
            ver = m.group(1)
            continue
        m = pat_dce.match(line)
        if m and ver is not None:
            out.write(f"{ver},{m.group(1)}\n")