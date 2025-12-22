import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

csv_folder = 'bin/sfhit_csv'
fig_folder = 'sf_fig'
os.makedirs(fig_folder, exist_ok=True)

# 遍历所有csv文件
for csv_file in glob.glob(f'{csv_folder}/*.csv'):
    # 提取版本号
    base_name = os.path.basename(csv_file)
    version = base_name.split('_v')[-1].split('.')[0]

    df = pd.read_csv(csv_file)

    # 排序并取Top100
    df = df.sort_values('HitCount', ascending=False).head(100)

    plt.figure(figsize=(16, 6))
    plt.bar(df['SuperFeature'].astype(str), df['HitCount'])
    plt.xlabel('super feature')
    plt.ylabel('hit frequency')
    plt.title(f'Version {version} Top100')
    plt.xticks(rotation=90, fontsize=8)
    plt.tight_layout()

    fig_path = f'{fig_folder}/sf_hits_top100_v{version}.png'
    plt.savefig(fig_path)
    plt.close()

    print(f'已保存: {fig_path}')