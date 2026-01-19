if [ -z "$1" ]; then
  echo "错误: 请提供 perf.data 文件的路径作为参数。"
  echo "用法: $0 path/to/your/perf.data"
  exit 1
fi

INPUT_FILE="$1"
# 根据输入文件名自动生成输出的SVG文件名
OUTPUT_FILE="${INPUT_FILE%.data}.svg"
FLAMEGRAPH_DIR="/home/public/YP/FlameGraph"

echo "正在处理: $INPUT_FILE"
echo "将要生成: $OUTPUT_FILE"

# 核心的一行命令
perf script -i "$INPUT_FILE" | "$FLAMEGRAPH_DIR/stackcollapse-perf.pl" | "$FLAMEGRAPH_DIR/flamegraph.pl" > "$OUTPUT_FILE"

echo "火焰图生成完毕: $OUTPUT_FILE"