import numpy as np
import matplotlib.pyplot as plt

# 读取数据（跳过表头）
distances = []
cdfs = []
with open('bin/linux/Insight11.txt', 'r') as f:
    for line in f:
        if line.startswith('#') or line.strip() == '':
            continue
        parts = line.strip().split()
        if len(parts) == 3:
            distance_val = int(parts[0])
            cdf_val = float(parts[2])
            # 只保留distance <= 500000的数据点
            if distance_val <= 1990000:
                distances.append(distance_val)
                cdfs.append(cdf_val)

# 画CDF曲线
plt.figure(figsize=(8,6))
plt.plot(distances, cdfs, marker='.',markersize=3, linestyle='-',linewidth=0.7, color='b', label='CDF')
plt.xlabel('distance')
plt.ylabel('CDF')
plt.title('linux set distance Insight')
plt.grid(True)
plt.legend()

# 可选：设置x轴范围确保只显示到500000
plt.xlim(0, 1990000)

# 可选：格式化横坐标显示，使其更易读
plt.gca().xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

plt.tight_layout()
plt.savefig('Insight10_cdf.png', dpi=300)
plt.show()