import matplotlib.pyplot as plt
import numpy as np
import os

def create_cdf_plots(input_folder="cdf_sf", output_folder="cdf_plots"):
    """
    为每个txt文件生成单独的CDF图并保存到文件夹
    
    参数:
    input_folder: 输入txt文件所在的文件夹路径
    output_folder: 输出图片的文件夹路径
    """
    # 创建输出文件夹（如果不存在）
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"创建输出文件夹: {output_folder}")
    
    # 获取所有cdf_sf开头的txt文件
    files = [f for f in os.listdir(input_folder) if f.startswith('cdf_sf_') and f.endswith('.txt')]
    
    if not files:
        print(f"在文件夹 {input_folder} 中未找到cdf_sf开头的txt文件")
        return
    
    print(f"找到 {len(files)} 个文件，开始生成CDF图...")
    
    for i, file in enumerate(files, 1):
        # 从文件名提取rootID
        root_id = file.replace('cdf_sf_', '').replace('.txt', '')
        file_path = os.path.join(input_folder, file)
        
        print(f"处理文件 {i}/{len(files)}: {file}")
        
        # 读取数据
        data = []
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:  # 确保不是空行
                    data.append(int(line))
        
        if not data:
            print(f"  文件 {file} 为空，跳过")
            continue
        
        # 计算CDF
        x_values = list(range(len(data)))  # 横轴：0, 1, 2, 3, ...
        cumulative_sum = 0
        cdf_values = []
        
        # 计算每个点的累积值
        for value in data:
            cumulative_sum += value
            cdf_values.append(cumulative_sum)
        
        # 归一化：除以总数
        total = cumulative_sum
        cdf_normalized = [val / total for val in cdf_values]
        
        # 创建图形
        plt.figure(figsize=(10, 6))
        
        # 绘制CDF曲线
        plt.plot(x_values, cdf_normalized, marker='o', markersize=2, linewidth=1.5, color='blue')
        
        # 设置图表属性
        plt.xlabel('Index', fontsize=12)
        plt.ylabel('Cumulative Probability', fontsize=12)
        plt.title(f'CDF for Super Feature (RootID: {root_id})', fontsize=14)
        plt.grid(True, alpha=0.3)
        
        # 添加统计信息文本框
        stats_text = f'数据点: {len(data)}\n总和: {total}\n范围: [{min(data)}, {max(data)}]'
        plt.annotate(stats_text, xy=(0.02, 0.98), xycoords='axes fraction', 
                    verticalalignment='top', fontsize=10,
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
        
        # 设置坐标轴范围
        plt.xlim(0, len(data))
        plt.ylim(0, 1.05)
        
        plt.tight_layout()
        
        # 保存图片
        output_path = os.path.join(output_folder, f"cdf_plot_{root_id}.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()  # 关闭图形以释放内存
        
        print(f"  已保存: cdf_plot_{root_id}.png")
    
    print(f"\n完成！所有图片已保存到 {output_folder} 文件夹")

def create_simple_cdf_plots(input_folder="cdf_sf", output_folder="cdf_plots_simple"):
    """
    简化版本的CDF图生成（无统计信息，更简洁）
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    files = [f for f in os.listdir(input_folder) if f.startswith('cdf_sf_') and f.endswith('.txt')]
    
    for file in files:
        root_id = file.replace('cdf_sf_', '').replace('.txt', '')
        file_path = os.path.join(input_folder, file)
        
        # 读取数据
        with open(file_path, 'r') as f:
            data = [int(line.strip()) for line in f if line.strip()]
        
        if not data:
            continue
        
        # 计算CDF
        cumulative = []
        total = 0
        for value in data:
            total += value
            cumulative.append(total)
        
        cdf = [c / total for c in cumulative]
        
        # 绘制简单图形
        plt.figure(figsize=(8, 5))
        plt.plot(range(len(data)), cdf, 'b-', linewidth=2)
        plt.xlabel('Index')
        plt.ylabel('Cumulative Probability')
        plt.title(f'CDF - RootID: {root_id}')
        plt.grid(True, alpha=0.3)
        
        # 保存
        output_path = os.path.join(output_folder, f"cdf_simple_{root_id}.png")
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

# 使用示例
if __name__ == "__main__":
    input_folder = "bin/cdf_sf"  # 输入文件夹
    output_folder = "cdf_plots"  # 输出文件夹
    
    # 检查输入文件夹是否存在
    if not os.path.exists(input_folder):
        print(f"错误：输入文件夹 {input_folder} 不存在")
        print("请确保C++代码已经运行并生成了txt文件")
    else:
        # 生成详细版本的CDF图
        # create_cdf_plots(input_folder, output_folder)
        
        # 如果需要简化版本，取消下面的注释
        create_simple_cdf_plots(input_folder, "cdf_plots_simple")
        
        print("\n所有图片生成完成！")