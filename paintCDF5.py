import numpy as np
import matplotlib.pyplot as plt

# 读取数据
data = np.loadtxt('insight/WEB/Insight5_size_vs_early_hit.txt', skiprows=1)
vector_size = data[:, 0]
early_hit_ratio = data[:, 1]

# 绘制散点图
plt.scatter(vector_size, early_hit_ratio, s=10, alpha=0.7)
plt.xlabel('set size')
plt.ylabel('early hit ratio')
plt.title('WEB Early Hit Ratio')
plt.grid()
plt.savefig('insight/WEB/Insight5_size_vs_early_hit_scatter.png', dpi=300, bbox_inches='tight')
plt.show()