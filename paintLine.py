import pandas as pd
import matplotlib.pyplot as plt
import os
import glob

csv_folder = 'bin/sf_csv'
fig_folder = 'sf_fig'
os.makedirs(fig_folder, exist_ok=True)

# 遍历所有 sf_access_seq_v*.csv 文件
for csv_file in glob.glob(f'{csv_folder}/sf_access_seq_v*.csv'):
    base_name = os.path.basename(csv_file)
    version = base_name.split('_v')[-1].split('.')[0]

    df = pd.read_csv(csv_file)
    plt.figure(figsize=(16, 6))
    plt.scatter(df['AccessIndex'], df['SF_ID'], s=8, alpha=0.6)  # s为点的大小，alpha为透明度
    plt.xlabel('AccessIndex')
    plt.ylabel('SF_ID')
    plt.title(f'SF Access Sequence Version {version}')
    plt.grid(True, linewidth=0.3)
    plt.tight_layout()

    fig_path = f'{fig_folder}/sf_access_seq_v{version}_scatter.png'
    plt.savefig(fig_path, dpi=200)
    plt.close()

    print(f'已保存: {fig_path}')