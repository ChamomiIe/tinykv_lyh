#!/bin/bash
# filepath: /home/tionfan/tinykv/run_test_basic2b.sh

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# 测试名称
TEST_NAME="TestBasic2B"
TOTAL_RUNS=100
PASSED=0
FAILED=0

# 创建单个日志文件
LOG_FILE="test_basic2b_runs.log"
# 清空日志文件
> $LOG_FILE

echo -e "${YELLOW}开始运行 $TEST_NAME 测试 $TOTAL_RUNS 次...${NC}"
echo "==============================================="

for i in $(seq 1 $TOTAL_RUNS); do
    TEMP_LOG_FILE=$(mktemp)

    echo -n "运行 #$i: "

    # 添加标记到日志文件
    echo "============== 测试运行 #$i ==============" >> $LOG_FILE
    echo "开始时间: $(date)" >> $LOG_FILE
    echo "" >> $LOG_FILE

    # 使用go test运行特定测试
    if go test -count=1 -v ./kv/test_raftstore -run $TEST_NAME -timeout 5m > $TEMP_LOG_FILE 2>&1; then
        echo -e "${GREEN}通过✓${NC}"
        ((PASSED++))
        echo "【结果: 通过】" >> $LOG_FILE
    else
        echo -e "${RED}失败✗${NC}"
        ((FAILED++))
        echo "【结果: 失败】" >> $LOG_FILE
        echo "" >> $LOG_FILE
        echo "错误详情:" >> $LOG_FILE
    fi

    # 将测试输出添加到主日志文件
    cat $TEMP_LOG_FILE >> $LOG_FILE

    # 添加三个空行作为分隔符
    echo "" >> $LOG_FILE
    echo "" >> $LOG_FILE
    echo "" >> $LOG_FILE

    # 删除临时文件
    rm $TEMP_LOG_FILE
done

echo "==============================================="
echo -e "${YELLOW}测试结果汇总:${NC}"
echo "总共运行: $TOTAL_RUNS"
echo -e "${GREEN}通过: $PASSED${NC}"
echo -e "${RED}失败: $FAILED${NC}"

# 如果有失败，显示失败的运行编号
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}失败的测试运行:${NC}"
    grep -n "【结果: 失败】" $LOG_FILE | cut -d':' -f1 | while read line; do
        # 提取测试运行编号
        test_num=$(grep -B2 -A0 -n "【结果: 失败】" $LOG_FILE | grep "测试运行" | head -1 | sed 's/.*测试运行 #\([0-9]*\).*/\1/')
        if [ ! -z "$test_num" ]; then
            echo "运行 #$test_num"
        fi
    done
fi

# 计算通过率
PASS_RATE=$(echo "scale=2; $PASSED*100/$TOTAL_RUNS" | bc)
echo -e "${YELLOW}通过率: ${PASS_RATE}%${NC}"
echo "详细日志保存在 $LOG_FILE"

exit $FAILED