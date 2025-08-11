Aurora数据增量计算方案，Cloudwatch 监控指标: VolumeBytesUsed：统计月末和月初数值，两者相减；
Aurora盘写入IOPS计算方案： Cloudwatch 指标 VolumeWriteIOPS: 根据1小时时间间隔做sum；
我要估算aurora global database的成本，需要统计 aurora所有实例的进30天的write io和每日新增数据量。用aws cli 从 cloud watch查询相关数据，并输出列表。 脚本要支持cloudshell运行，不指定region，从cloudshell运行环境获取当前region。

将输出字段调整成 集群、实例、月初的存储总量(原始值)
集群名称,实例ID,引擎,实例类型,使用Secret Manager,30天总写IO次数,月初的存储总量(原始值),月初的存储总量(GB),月末存储总量(原始值),月末的存储总量(GB)