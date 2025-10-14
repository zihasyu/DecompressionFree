import numpy as np
import matplotlib.pyplot as plt

data = np.loadtxt('insight/WindowsLog/Insight1_vector_size_cdf.txt', skiprows=1)
vector_size = data[:, 0]
cdf = data[:, 2]

# 在数据前加上 (0, 0)
vector_size = np.insert(vector_size, 0, 0)
cdf = np.insert(cdf, 0, 0)

# 只显示前99.5%的数据
mask = cdf <= 0.995
plt.plot(vector_size[mask], cdf[mask], marker='o', markersize=1, linewidth=1)
plt.xlabel('set size')
plt.ylabel('CDF')
plt.title('WindowsLog similarity sets(99.5%)')
plt.grid()
plt.ylim(bottom=0)

plt.savefig('insight/WindowsLog/Insight1_vector_size_cdf_995.png', dpi=300, bbox_inches='tight')
plt.show()