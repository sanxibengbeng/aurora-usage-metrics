# Aurora Global Database 成本分析工具

这个工具用于分析Aurora Global Database的成本，通过查询CloudWatch获取过去30天的写IO和数据增长量数据。

## 文件说明

- `aurora_cost_analysis.py` - Python脚本
- `README.md` - 使用说明

## 功能特性

- 自动获取当前region（适合CloudShell环境）
- 查询所有Aurora集群和实例
- 统计过去30天的写IO次数
- 估算数据增长量
- 生成CSV格式的详细报告
- 提供汇总统计信息

## 使用方法

### 在CloudShell中运行

```bash
python3 aurora_cost_analysis.py
```

### 输出文件

脚本会生成一个CSV文件：`aurora_cost_analysis_YYYYMMDD_HHMMSS.csv`

CSV文件包含以下列：
- 集群名称
- 实例ID
- 引擎类型
- 实例类型
- 30天总写IO次数
- 30天平均每日写IO
- 30天数据增长(GB)
- 平均每日数据增长(GB)

## 前提条件

1. 在AWS CloudShell中运行，或者本地配置了AWS CLI
2. 具有以下权限：
   - `rds:DescribeDBClusters`
   - `rds:DescribeDBInstances`
   - `cloudwatch:GetMetricStatistics`

## 注意事项

1. **数据增长估算**：通过FreeStorageSpace的变化来估算数据增长，可能不够精确
2. **Global Database成本**：跨区域复制会产生额外的写IO成本
3. **时间范围**：查询过去30天的数据，如果实例创建时间少于30天，数据可能不完整
4. **CloudWatch延迟**：CloudWatch指标可能有延迟，最新的数据可能不可用

## 成本估算建议

1. 使用脚本获取的写IO数据来估算Aurora Global Database的写IO成本
2. 结合AWS Pricing Calculator进行详细的成本估算：https://calculator.aws
3. 考虑以下成本因素：
   - 主区域的写IO成本
   - 跨区域复制的网络传输成本
   - 备份存储成本
   - 实例运行成本

## 故障排除

如果遇到权限错误，请确保：
1. AWS凭证已正确配置
2. 具有必要的IAM权限
3. 在正确的region中运行

如果没有找到Aurora集群，请检查：
1. 当前region是否有Aurora集群
2. 集群是否为Aurora MySQL或PostgreSQL引擎
