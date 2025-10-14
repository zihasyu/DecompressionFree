import numpy as np
import matplotlib.pyplot as plt

# 读取数据
data = np.loadtxt('insight/WindowsLog/Insight2.txt', skiprows=1)
reverse_pos = data[:, 0] + 1  # 每个横坐标加1
cdf = data[:, 2]

# 在数据前加上 (0, 0)
reverse_pos = np.insert(reverse_pos, 0, 0)
cdf = np.insert(cdf, 0, 0)

# 绘制 CDF 图
plt.plot(reverse_pos, cdf, marker='o', markersize=1, linewidth=1)
plt.xlabel('base chunk position')
plt.ylabel('CDF')
plt.title('WindowsLog best base chunk')
plt.grid()
plt.ylim(bottom=0)

# 保存图片
plt.savefig('insight/WindowsLog/Insight2_cdf.png', dpi=300, bbox_inches='tight')
plt.show()