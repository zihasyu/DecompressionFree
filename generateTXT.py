import os

input_file = 'insight/WEB/Insight6_super_large_sets.txt'

# 用于存储每个set的数据
sets_data = {}

with open(input_file, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith('#'):
            continue  # 跳过空行和以#开头的行
        parts = line.split()
        if len(parts) < 3:
            continue
        set_name = parts[0]
        try:
            col2 = float(parts[1])
            col3 = float(parts[2])
            ratio = col3 / col2 if col2 != 0 else 'inf'
        except ValueError:
            continue
        sets_data.setdefault(set_name, []).append(str(ratio))

# 生成每个set的txt文件
for set_name, ratios in sets_data.items():
    output_file = f'{set_name}.txt'
    with open(output_file, 'w', encoding='utf-8') as f:
        for ratio in ratios:
            f.write(ratio + '\n')

print("处理完成，已生成各set的txt文件。")