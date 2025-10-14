import numpy as np
import matplotlib.pyplot as plt

# 读取数据
data = np.loadtxt('insight/WindowsLog/Insight2.txt', skiprows=1)
reverse_pos = data[:, 0] + 1  # 每个横坐标加1
cdf = data[:, 2]

# 在数据前加上 (0, 0)
reverse_pos = np.insert(reverse_pos, 0, 0)
cdf = np.insert(cdf, 0, 0)

# 只显示前99.5%的数据
mask = cdf <= 0.995
plt.plot(reverse_pos[mask], cdf[mask], marker='o', markersize=1, linewidth=1)
plt.xlabel('base chunk position')
plt.ylabel('CDF')
plt.title('WindowsLog best base chunk(Top 99.5%)')
plt.grid()
plt.ylim(bottom=0)

plt.savefig('insight/WindowsLog/Insight2_cdf_995.png', dpi=300, bbox_inches='tight')
plt.show()