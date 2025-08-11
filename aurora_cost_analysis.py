#!/usr/bin/env python3
"""
Aurora Global Database 成本分析脚本
查询过去30天的Write IO和数据增长量
"""

import boto3
import json
import csv
from datetime import datetime, timedelta, timezone
import sys
from botocore.exceptions import ClientError, NoCredentialsError

def get_current_region():
    """获取当前region"""
    try:
        session = boto3.Session()
        return session.region_name
    except Exception:
        return None

def get_aurora_clusters(rds_client):
    """获取所有Aurora集群"""
    try:
        response = rds_client.describe_db_clusters()
        aurora_clusters = []
        
        for cluster in response['DBClusters']:
            if cluster['Engine'] in ['aurora-mysql', 'aurora-postgresql']:
                # 检查是否使用了Secret Manager
                uses_secret_manager = bool(cluster.get('MasterUserSecret'))
                secret_arn = cluster.get('MasterUserSecret', {}).get('SecretArn', '') if uses_secret_manager else ''
                
                aurora_clusters.append({
                    'identifier': cluster['DBClusterIdentifier'],
                    'engine': cluster['Engine'],
                    'engine_version': cluster['EngineVersion'],
                    'uses_secret_manager': uses_secret_manager,
                    'secret_arn': secret_arn
                })
        
        return aurora_clusters
    except ClientError as e:
        print(f"错误获取Aurora集群: {e}")
        return []

def get_cluster_instances(rds_client, cluster_id):
    """获取集群中的实例"""
    try:
        response = rds_client.describe_db_instances()
        instances = []
        
        for instance in response['DBInstances']:
            if instance.get('DBClusterIdentifier') == cluster_id:
                instances.append({
                    'identifier': instance['DBInstanceIdentifier'],
                    'instance_class': instance['DBInstanceClass'],
                    'engine': instance['Engine']
                })
        
        return instances
    except ClientError as e:
        print(f"错误获取实例信息: {e}")
        return []

def get_cluster_metric_data(cloudwatch_client, metric_name, cluster_id, start_time, end_time, statistic='Average', period=86400):
    """从CloudWatch获取集群级别的指标数据"""
    try:
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/RDS',
            MetricName=metric_name,
            Dimensions=[
                {
                    'Name': 'DBClusterIdentifier',
                    'Value': cluster_id
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=[statistic]
        )
        
        if response['Datapoints']:
            # 按时间排序
            sorted_datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
            return [(point['Timestamp'], point[statistic]) for point in sorted_datapoints]
        return []
        
    except ClientError as e:
        print(f"错误获取集群指标 {metric_name} for {cluster_id}: {e}")
        return []

def get_metric_data(cloudwatch_client, metric_name, instance_id, start_time, end_time, statistic='Sum', period=86400):
    """从CloudWatch获取指标数据"""
    try:
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/RDS',
            MetricName=metric_name,
            Dimensions=[
                {
                    'Name': 'DBInstanceIdentifier',
                    'Value': instance_id
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=[statistic]
        )
        
        if response['Datapoints']:
            # 按时间排序
            sorted_datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
            return [(point['Timestamp'], point[statistic]) for point in sorted_datapoints]
        return []
        
    except ClientError as e:
        print(f"错误获取指标 {metric_name} for {instance_id}: {e}")
        return []
    """从CloudWatch获取指标数据"""
    try:
        response = cloudwatch_client.get_metric_statistics(
            Namespace='AWS/RDS',
            MetricName=metric_name,
            Dimensions=[
                {
                    'Name': 'DBInstanceIdentifier',
                    'Value': instance_id
                }
            ],
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=[statistic]
        )
        
        if response['Datapoints']:
            # 按时间排序
            sorted_datapoints = sorted(response['Datapoints'], key=lambda x: x['Timestamp'])
            return [(point['Timestamp'], point[statistic]) for point in sorted_datapoints]
        return []
        
    except ClientError as e:
        print(f"错误获取指标 {metric_name} for {instance_id}: {e}")
        return []

def calculate_write_io_stats(write_io_data):
    """计算写IO统计 - 累加所有时间间隔的值"""
    if not write_io_data:
        return 0, 0
    
    # write_io_data 现在是 [(timestamp, value), ...] 的格式
    # 累加所有间隔的写IO值
    total_write_io = sum(value for timestamp, value in write_io_data)
    avg_daily_write_io = total_write_io / len(write_io_data) if write_io_data else 0
    
    return int(total_write_io), int(avg_daily_write_io)

def calculate_storage_growth(volume_bytes_data):
    """计算存储增长（使用VolumeBytesUsed，月末减月初）"""
    if len(volume_bytes_data) < 2:
        return "N/A", "N/A"
    
    # volume_bytes_data 现在是 [(timestamp, value), ...] 的格式，已按时间排序
    # 获取月初（最早）和月末（最晚）的数据
    first_timestamp, first_value = volume_bytes_data[0]  # 月初
    last_timestamp, last_value = volume_bytes_data[-1]   # 月末
    
    # 转换为GB
    first_value_gb = first_value / (1024**3)
    last_value_gb = last_value / (1024**3)
    
    # 计算增长量（月末 - 月初）
    data_growth = last_value_gb - first_value_gb if last_value_gb > first_value_gb else 0
    
    # 计算时间差（天数）
    time_diff = (last_timestamp - first_timestamp).days
    avg_daily_growth = data_growth / time_diff if time_diff > 0 else 0
    
    return round(data_growth, 2), round(avg_daily_growth, 2)

def main():
    print("Aurora Global Database 成本分析脚本")
    print("=" * 50)
    
    # 获取当前region
    region = get_current_region()
    if not region:
        print("错误: 无法获取当前region，请确保在CloudShell中运行或配置了默认region")
        sys.exit(1)
    
    print(f"当前Region: {region}")
    
    # 初始化AWS客户端
    try:
        rds_client = boto3.client('rds', region_name=region)
        cloudwatch_client = boto3.client('cloudwatch', region_name=region)
    except NoCredentialsError:
        print("错误: AWS凭证未配置")
        sys.exit(1)
    
    # 计算时间范围（过去30天）
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=30)
    
    print(f"查询时间范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} 到 {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 获取Aurora集群
    print("正在获取Aurora集群列表...")
    clusters = get_aurora_clusters(rds_client)
    
    if not clusters:
        print("未找到Aurora集群")
        sys.exit(0)
    
    print(f"找到 {len(clusters)} 个Aurora集群")
    print()
    
    # 准备结果数据
    results = []
    
    # 分析每个集群
    for cluster in clusters:
        cluster_id = cluster['identifier']
        uses_secret_manager = cluster['uses_secret_manager']
        secret_arn = cluster['secret_arn']
        
        print(f"分析集群: {cluster_id} ({cluster['engine']})")
        if uses_secret_manager:
            print(f"  使用Secret Manager: 是")
        else:
            print(f"  使用Secret Manager: 否")
        
        # 获取集群级别的存储数据
        print("  查询集群存储使用数据...")
        cluster_volume_bytes_data = get_cluster_metric_data(
            cloudwatch_client, 'VolumeBytesUsed', cluster_id,
            start_time, end_time, 'Average', period=86400  # 1天间隔
        )
        
        cluster_data_growth, cluster_avg_daily_growth = calculate_storage_growth(cluster_volume_bytes_data)
        
        # 获取集群实例
        instances = get_cluster_instances(rds_client, cluster_id)
        
        if not instances:
            print(f"  集群 {cluster_id} 中未找到实例")
            continue
        
        for instance in instances:
            instance_id = instance['identifier']
            print(f"  分析实例: {instance_id}")
            
            # 获取写IO数据 - 先尝试VolumeWriteIOPS，如果没有数据则尝试WriteIOPS
            print("    查询写IO数据...")
            write_io_data = get_metric_data(
                cloudwatch_client, 'VolumeWriteIOPS', instance_id, 
                start_time, end_time, 'Sum', period=3600  # 1小时间隔
            )
            
            # 如果VolumeWriteIOPS没有数据，尝试WriteIOPS
            if not write_io_data:
                print("    VolumeWriteIOPS无数据，尝试WriteIOPS...")
                write_io_data = get_metric_data(
                    cloudwatch_client, 'WriteIOPS', instance_id, 
                    start_time, end_time, 'Sum', period=3600  # 1小时间隔
                )
            
            total_write_io, avg_daily_write_io = calculate_write_io_stats(write_io_data)
            
            # 使用集群级别的存储数据
            data_growth = cluster_data_growth
            avg_daily_growth = cluster_avg_daily_growth
            
            # 保存结果
            result = {
                '集群名称': cluster_id,
                '实例ID': instance_id,
                '引擎': instance['engine'],
                '实例类型': instance['instance_class'],
                '使用Secret Manager': '是' if uses_secret_manager else '否',
                '30天总写IO次数': total_write_io,
                '30天平均每日写IO': avg_daily_write_io,
                '30天数据增长(GB)': data_growth,
                '平均每日数据增长(GB)': avg_daily_growth
            }
            
            results.append(result)
            
            print(f"    写IO总数: {total_write_io:,}, 平均每日: {avg_daily_write_io:,}")
            print(f"    数据增长: {data_growth}GB, 平均每日: {avg_daily_growth}GB")
            print()
    
    # 保存结果到CSV文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f'aurora_cost_analysis_{timestamp}.csv'
    
    if results:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = results[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        
        print("=" * 50)
        print(f"结果已保存到: {csv_filename}")
        print()
        
        # 生成汇总报告
        print("汇总报告:")
        print("=" * 30)
        
        total_instances = len(results)
        total_write_io_all = sum(r['30天总写IO次数'] for r in results)
        avg_daily_write_io_all = total_write_io_all // 30
        
        # 计算总数据增长
        valid_growth = [r['30天数据增长(GB)'] for r in results if r['30天数据增长(GB)'] != 'N/A']
        total_data_growth = sum(valid_growth) if valid_growth else 0
        
        print(f"总Aurora实例数: {total_instances}")
        print(f"30天总写IO次数: {total_write_io_all:,}")
        print(f"平均每日写IO次数: {avg_daily_write_io_all:,}")
        print(f"30天总数据增长: {total_data_growth:.2f} GB")
        print(f"平均每日数据增长: {total_data_growth/30:.2f} GB")
        
        print()
        print("注意事项:")
        print("1. 写IO统计：使用VolumeWriteIOPS指标，按小时间隔累加所有时间段的写IO次数")
        print("2. 数据增长计算：使用VolumeBytesUsed指标，计算月末与月初数值的差值")
        print("3. Global Database成本：跨区域复制会产生额外的写IO成本")
        print("4. 可以使用AWS Pricing Calculator进一步估算成本: https://calculator.aws")
        
        # 显示前几行数据
        print()
        print("数据预览:")
        print("-" * 120)
        for i, result in enumerate(results[:5]):
            secret_info = f"Secret: {result['使用Secret Manager']}"
            print(f"{i+1}. {result['集群名称']}/{result['实例ID']} - "
                  f"写IO: {result['30天总写IO次数']:,}, "
                  f"数据增长: {result['30天数据增长(GB)']}GB, "
                  f"{secret_info}")
        
        if len(results) > 5:
            print(f"... 还有 {len(results) - 5} 个实例")
    
    else:
        print("未找到任何Aurora实例数据")

if __name__ == "__main__":
    main()
