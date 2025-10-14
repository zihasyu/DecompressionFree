import numpy as np
import matplotlib.pyplot as plt

# 读取数据
data = np.loadtxt('insight/WindowsLog/Insight1_vector_size_cdf.txt', skiprows=1)
vector_size = data[:, 0]
cdf = data[:, 2]

# 在数据前加上 (0, 0)
vector_size = np.insert(vector_size, 0, 0)
cdf = np.insert(cdf, 0, 0)

# 绘制 CDF 图
plt.plot(vector_size, cdf, marker='o', markersize=1, linewidth=1)
plt.xlabel('set size')
plt.ylabel('CDF')
plt.title('WindowsLog similarity sets')
plt.grid()
plt.ylim(bottom=0)

# 保存图片
plt.savefig('insight/WindowsLog/Insight1_vector_size_cdf.png', dpi=300, bbox_inches='tight')

plt.show()