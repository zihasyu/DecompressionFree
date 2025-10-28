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
            distances.append(int(parts[0]))
            cdfs.append(float(parts[2]))

distances = np.array(distances)
cdfs = np.array(cdfs)

# 截取到 99.5%（0.995）
target = 0.995
if cdfs.max() < target:
    # 数据未达到 99.5%，画全部并提示
    print(f"警告：数据最大 CDF={cdfs.max():.6f} < {target}，将画出全部数据。")
    d_trim = distances
    c_trim = cdfs
else:
    # 找到第一个 >= target 的索引并插值到精确的 target
    idx = np.argmax(cdfs >= target)
    if cdfs[idx] == target or idx == 0:
        # 恰好或在第一个点处
        d_trim = distances[:idx+1]
        c_trim = cdfs[:idx+1]
    else:
        # 在 idx-1 和 idx 之间插值计算距离值，使 cdf 精确到 target
        x0, y0 = distances[idx-1], cdfs[idx-1]
        x1, y1 = distances[idx], cdfs[idx]
        # 线性插值 x_at 对应 y=target
        x_at = x0 + (target - y0) * (x1 - x0) / (y1 - y0)
        d_trim = np.concatenate([distances[:idx], [x_at]])
        c_trim = np.concatenate([cdfs[:idx], [target]])

# 画CDF曲线（只到 99.5%）
plt.figure(figsize=(8,6))
plt.plot(d_trim, c_trim, marker='.', markersize=3, linestyle='-', linewidth=0.7, color='b', label='CDF (<=99.5%)')
plt.xlabel('distance')
plt.ylabel('CDF')
plt.title('linux set distance insight (99.5%)')
plt.grid(True)
plt.legend()
plt.xlim(left=d_trim[0], right=d_trim[-1])
plt.ylim(0, target + 0.001)  # 轻微上边距
plt.tight_layout()
plt.savefig('Insight10_cdf_99.5.png', dpi=300)
plt.show()