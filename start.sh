#!/bin/bash
# 抖音多直播间监控系统 - 启动脚本
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# 检查依赖
if [ ! -d "node_modules" ]; then
  echo ">>> 安装 Node.js 依赖..."
  npm install
fi

# 检查 Python F2 (可选)
python3 -c "import f2" 2>/dev/null || echo "⚠️  F2 未安装，主播信息获取功能不可用 (pip3 install f2)"

# 数据 & 日志目录
mkdir -p logs data

# 启动服务
echo ">>> 启动服务: http://localhost:${PORT:-3000}"
node server/index.js >> logs/server.log 2>&1 &
echo $! > .pid
echo ">>> PID: $(cat .pid)"
