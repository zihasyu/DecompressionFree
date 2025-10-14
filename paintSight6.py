import matplotlib.pyplot as plt
from collections import defaultdict

# 读取数据
data = defaultdict(list)

with open('insight/WEB/Insight6_super_large_sets.txt', 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#') or line.startswith('set_id'):
            continue  # 跳过表头和空行
        parts = line.split()
        if len(parts) != 3:
            continue  # 跳过格式不对的行
        set_id, chunk_id, basechunk_id = parts[0], int(parts[1]), int(parts[2])
        value = 0 if basechunk_id == -1 else basechunk_id / chunk_id
        data[set_id].append((chunk_id, value))

# 绘图
for set_id, values in data.items():
    values.sort(key=lambda x: x[0])
    chunk_ids = [v[0] for v in values]
    y_values = [v[1] for v in values]
    plt.figure()
    plt.plot(chunk_ids, y_values, marker='o',markersize=1, linewidth=1)
    plt.title(f'Set: {set_id}')
    plt.xlabel('chunk id')
    plt.ylabel('basechunk id / chunk id')
    plt.grid(True)
    plt.savefig(f'{set_id}_lineplot.png')
    # plt.show()

print("绘图完成，图片已保存为 {set_id}_lineplot.png 格式。")