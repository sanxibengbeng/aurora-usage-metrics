#!/bin/bash

# Aurora Global Database 成本分析脚本
# 查询过去30天的Write IO和数据增长量

set -e

# 获取当前region（CloudShell环境）
REGION=$(aws configure get region)
if [ -z "$REGION" ]; then
    echo "错误: 无法获取当前region，请确保在CloudShell中运行或配置了默认region"
    exit 1
fi

echo "当前Region: $REGION"
echo "开始分析Aurora实例..."

# 计算时间范围（过去30天）
END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
START_TIME=$(date -u -d '30 days ago' +"%Y-%m-%dT%H:%M:%S")

echo "查询时间范围: $START_TIME 到 $END_TIME"
echo "=================================="

# 获取所有Aurora集群
echo "正在获取Aurora集群列表..."
CLUSTERS=$(aws rds describe-db-clusters --region $REGION --query 'DBClusters[?Engine==`aurora-mysql` || Engine==`aurora-postgresql`].DBClusterIdentifier' --output text)

if [ -z "$CLUSTERS" ]; then
    echo "未找到Aurora集群"
    exit 0
fi

# 创建结果文件
RESULT_FILE="aurora_cost_analysis_$(date +%Y%m%d_%H%M%S).csv"
echo "集群名称,实例ID,引擎,实例类型,30天总写IO次数,30天平均每日写IO,30天数据增长(GB),平均每日数据增长(GB)" > $RESULT_FILE

echo "结果将保存到: $RESULT_FILE"
echo ""

# 遍历每个集群
for CLUSTER in $CLUSTERS; do
    echo "分析集群: $CLUSTER"
    
    # 获取集群中的实例
    INSTANCES=$(aws rds describe-db-instances --region $REGION --query "DBInstances[?DBClusterIdentifier=='$CLUSTER'].DBInstanceIdentifier" --output text)
    
    if [ -z "$INSTANCES" ]; then
        echo "  集群 $CLUSTER 中未找到实例"
        continue
    fi
    
    # 获取集群信息
    CLUSTER_INFO=$(aws rds describe-db-clusters --region $REGION --db-cluster-identifier $CLUSTER --query 'DBClusters[0].[Engine,EngineVersion]' --output text)
    ENGINE=$(echo $CLUSTER_INFO | cut -f1)
    
    for INSTANCE in $INSTANCES; do
        echo "  分析实例: $INSTANCE"
        
        # 获取实例信息
        INSTANCE_INFO=$(aws rds describe-db-instances --region $REGION --db-instance-identifier $INSTANCE --query 'DBInstances[0].DBInstanceClass' --output text)
        
        # 查询写IO指标 (WriteIOPS)
        echo "    查询写IO数据..."
        WRITE_IO_DATA=$(aws cloudwatch get-metric-statistics \
            --region $REGION \
            --namespace AWS/RDS \
            --metric-name WriteIOPS \
            --dimensions Name=DBInstanceIdentifier,Value=$INSTANCE \
            --start-time $START_TIME \
            --end-time $END_TIME \
            --period 86400 \
            --statistics Sum \
            --query 'Datapoints[].Sum' \
            --output text 2>/dev/null || echo "")
        
        # 计算总写IO和平均每日写IO
        if [ -n "$WRITE_IO_DATA" ] && [ "$WRITE_IO_DATA" != "None" ]; then
            TOTAL_WRITE_IO=$(echo $WRITE_IO_DATA | tr ' ' '\n' | awk '{sum += $1} END {printf "%.0f", sum}')
            AVG_DAILY_WRITE_IO=$(echo $WRITE_IO_DATA | tr ' ' '\n' | awk '{sum += $1; count++} END {if(count>0) printf "%.0f", sum/count; else print "0"}')
        else
            TOTAL_WRITE_IO="0"
            AVG_DAILY_WRITE_IO="0"
        fi
        
        # 查询数据库大小变化 (DatabaseConnections作为替代，实际应该用FreeStorageSpace的变化)
        echo "    查询存储使用数据..."
        STORAGE_DATA=$(aws cloudwatch get-metric-statistics \
            --region $REGION \
            --namespace AWS/RDS \
            --metric-name FreeStorageSpace \
            --dimensions Name=DBInstanceIdentifier,Value=$INSTANCE \
            --start-time $START_TIME \
            --end-time $END_TIME \
            --period 86400 \
            --statistics Average \
            --query 'Datapoints[].Average' \
            --output text 2>/dev/null || echo "")
        
        # 计算数据增长（通过空闲存储空间的减少来估算）
        if [ -n "$STORAGE_DATA" ] && [ "$STORAGE_DATA" != "None" ]; then
            # 将字节转换为GB，并计算增长量
            STORAGE_VALUES=$(echo $STORAGE_DATA | tr ' ' '\n' | awk '{print $1/1024/1024/1024}')
            FIRST_VALUE=$(echo $STORAGE_VALUES | head -n1)
            LAST_VALUE=$(echo $STORAGE_VALUES | tail -n1)
            
            if [ -n "$FIRST_VALUE" ] && [ -n "$LAST_VALUE" ]; then
                DATA_GROWTH=$(echo "$FIRST_VALUE $LAST_VALUE" | awk '{diff = $1 - $2; if(diff > 0) printf "%.2f", diff; else print "0"}')
                AVG_DAILY_GROWTH=$(echo "$DATA_GROWTH" | awk '{printf "%.2f", $1/30}')
            else
                DATA_GROWTH="N/A"
                AVG_DAILY_GROWTH="N/A"
            fi
        else
            DATA_GROWTH="N/A"
            AVG_DAILY_GROWTH="N/A"
        fi
        
        # 输出结果
        echo "    写IO总数: $TOTAL_WRITE_IO, 平均每日: $AVG_DAILY_WRITE_IO"
        echo "    数据增长: ${DATA_GROWTH}GB, 平均每日: ${AVG_DAILY_GROWTH}GB"
        
        # 写入CSV文件
        echo "$CLUSTER,$INSTANCE,$ENGINE,$INSTANCE_INFO,$TOTAL_WRITE_IO,$AVG_DAILY_WRITE_IO,$DATA_GROWTH,$AVG_DAILY_GROWTH" >> $RESULT_FILE
        
        echo ""
    done
done

echo "=================================="
echo "分析完成！结果已保存到: $RESULT_FILE"
echo ""
echo "CSV文件内容预览:"
head -n 10 $RESULT_FILE

# 生成汇总报告
echo ""
echo "=================================="
echo "汇总报告:"
echo "=================================="

# 统计总实例数
TOTAL_INSTANCES=$(tail -n +2 $RESULT_FILE | wc -l)
echo "总Aurora实例数: $TOTAL_INSTANCES"

# 统计总写IO
TOTAL_WRITE_IO_ALL=$(tail -n +2 $RESULT_FILE | cut -d',' -f5 | awk '{sum += $1} END {printf "%.0f", sum}')
echo "30天总写IO次数: $TOTAL_WRITE_IO_ALL"

# 平均每日写IO
AVG_DAILY_WRITE_IO_ALL=$(echo "$TOTAL_WRITE_IO_ALL" | awk '{printf "%.0f", $1/30}')
echo "平均每日写IO次数: $AVG_DAILY_WRITE_IO_ALL"

echo ""
echo "注意事项:"
echo "1. 数据增长量通过FreeStorageSpace变化估算，可能不够精确"
echo "2. 建议结合Aurora的实际存储使用量进行验证"
echo "3. Global Database的跨区域复制会产生额外的写IO成本"
echo "4. 可以使用AWS Pricing Calculator进一步估算成本: https://calculator.aws"
