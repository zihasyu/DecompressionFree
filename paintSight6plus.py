import numpy as np
import matplotlib.pyplot as plt

# 读取数据
values = []
with open('5.txt', 'r', encoding='utf-8') as f:
    for line in f:
        try:
            values.append(float(line.strip()))
        except:
            continue

values = np.array(values)
values.sort()

# 计算CDF
cdf = np.arange(1, len(values)+1) / len(values)

plt.figure(figsize=(8,5))
plt.plot(values, cdf, marker='.', linestyle='-', linewidth=1)
plt.xlabel('base chunk id / chunk id')
plt.ylabel('CDF')
plt.title('CDF of values in set5')
plt.grid(True)
plt.tight_layout()
plt.savefig('5_cdf.png')
plt.show()