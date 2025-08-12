# Aurora & RDS 数据库成本分析工具

这个工具用于分析Aurora集群和RDS实例的成本，通过查询CloudWatch获取过去30天的写IO和数据增长量数据。

## 文件说明

- `aurora_cost_analysis.py` - Python脚本
- `run_analysis.sh` - 启动脚本（自动处理虚拟环境）
- `README.md` - 使用说明
- `.gitignore` - Git忽略文件配置

## 功能特性

- 自动获取当前region（适合CloudShell环境）
- 查询所有Aurora集群和RDS实例（支持分页，无数量限制）
- **数据脱敏**：所有数据库标识符使用SHA256哈希前8位进行脱敏
- **实例角色显示**：Aurora实例显示Writer/Reader角色，RDS实例显示Standalone
- 统计过去30天的写IO次数（Aurora使用VolumeWriteIOPS，RDS使用WriteIOPS）
- 计算Aurora集群数据增长量（VolumeBytesUsed月末减月初）
- 检测Secret Manager使用情况
- 生成CSV格式的详细报告
- 提供汇总统计信息

## 使用方法

### 在CloudShell中运行

```bash
python3 aurora_cost_analysis.py
```

### 输出文件

脚本会生成一个CSV文件：`aurora_rds_cost_analysis_YYYYMMDD_HHMMSS.csv`

CSV文件包含以下列：
- 数据库类型（Aurora集群/RDS实例）
- 集群/实例名称（脱敏后）
- 实例ID（脱敏后）
- 实例角色（Writer/Reader/Standalone）
- 引擎类型
- 实例类型
- 使用Secret Manager
- 30天总写IO次数
- 月初的存储总量(原始值)
- 月初的存储总量(GB)
- 月末存储总量(原始值)
- 月末的存储总量(GB)

## 前提条件

1. 在AWS CloudShell中运行，或者本地配置了AWS CLI
2. 具有以下权限：
   - `rds:DescribeDBClusters`
   - `rds:DescribeDBInstances`
   - `cloudwatch:GetMetricStatistics`

## 注意事项

1. **数据脱敏**：所有数据库标识符使用SHA256哈希处理，确保数据安全
2. **分页支持**：支持获取超过100个集群/实例的环境
3. **写IO统计**：
   - Aurora：使用VolumeWriteIOPS指标，按小时间隔累加所有时间段的写IO次数
   - RDS：使用WriteIOPS指标
4. **数据增长计算**：仅支持Aurora集群，使用VolumeBytesUsed指标计算月末与月初数值的差值
5. **实例角色**：Aurora实例显示Writer/Reader角色，RDS实例显示Standalone
6. **Global Database成本**：跨区域复制会产生额外的写IO成本
7. **时间范围**：查询过去30天的数据，如果实例创建时间少于30天，数据可能不完整
8. **CloudWatch延迟**：CloudWatch指标可能有延迟，最新的数据可能不可用

## 成本估算建议

1. 使用脚本获取的写IO数据来估算Aurora Global Database的写IO成本
2. 结合AWS Pricing Calculator进行详细的成本估算：https://calculator.aws
3. 考虑以下成本因素：
   - 主区域的写IO成本
   - 跨区域复制的网络传输成本
   - 备份存储成本
   - 实例运行成本

## 数据脱敏说明

为了保护敏感信息，脚本对所有数据库标识符进行脱敏处理：
- 使用SHA256哈希算法
- 取哈希值的前8位
- 格式：`db-xxxxxxxx`

例如：`my-production-cluster` → `db-0c711ae3`

## 故障排除

如果遇到权限错误，请确保：
1. AWS凭证已正确配置
2. 具有必要的IAM权限
3. 在正确的region中运行

如果没有找到数据库，请检查：
1. 当前region是否有Aurora集群或RDS实例
2. 集群/实例是否为支持的引擎类型

## 支持的数据库引擎

- Aurora MySQL
- Aurora PostgreSQL
- RDS MySQL
- RDS PostgreSQL
- RDS MariaDB
- RDS Oracle
- RDS SQL Server
