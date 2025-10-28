import numpy as np
import matplotlib.pyplot as plt

# 读取数据（跳过表头）
distances = []
cdfs = []
with open('bin/linux/Insight10_positive_only.txt', 'r') as f:
    for line in f:
        if line.startswith('#') or line.strip() == '':
            continue
        parts = line.strip().split()
        if len(parts) == 3:
            distances.append(int(parts[0]))
            cdfs.append(float(parts[2]))

# 画CDF曲线
plt.figure(figsize=(8,6))
plt.plot(distances, cdfs, marker='.',markersize=3, linestyle='-',linewidth=0.7, color='b', label='CDF')
plt.xlabel('diff')
plt.ylabel('CDF')
plt.title('linux relationship insight')
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.savefig('Insight10_cdf.png', dpi=300)
plt.show()