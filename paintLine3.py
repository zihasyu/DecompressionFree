import re
import matplotlib.pyplot as plt

ratios = []
versions = []

with open('insight/AllGreedy_WindowsLog.txt', 'r', encoding='utf-8') as f:
    for line in f:
        # 匹配 Insight3 Ratio 行
        m = re.search(r'Insight3 Hit Consistency Stats:.*Ratio: ([0-9.]+)', line)
        if m:
            ratio = float(m.group(1))
            ratios.append(ratio)
        # 匹配 Version 行
        v = re.search(r'Version: (\d+)', line)
        if v:
            versions.append(int(v.group(1)))

# 画折线图
plt.plot(versions, ratios, marker='o', markersize=1, linewidth=1)
plt.xlabel('Version')
plt.ylabel('Ratio')
plt.title('WindowsLog Hit Ratio per Version')
plt.grid()
plt.savefig('insight/AllGreedy_WindowsLog_insight3_ratio.png', dpi=300, bbox_inches='tight')
plt.show()