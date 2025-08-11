#!/usr/bin/env python3
"""
Aurora Global Database 成本分析脚本
查询过去30天的Write IO和数据增长量
"""

import boto3
import json
import csv
from datetime import datetime, timedelta
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
                aurora_clusters.append({
                    'identifier': cluster['DBClusterIdentifier'],
                    'engine': cluster['Engine'],
                    'engine_version': cluster['EngineVersion']
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

def get_metric_data(cloudwatch_client, metric_name, instance_id, start_time, end_time, statistic='Sum'):
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
            Period=86400,  # 1天
            Statistics=[statistic]
        )
        
        if response['Datapoints']:
            return [point[statistic] for point in response['Datapoints']]
        return []
        
    except ClientError as e:
        print(f"错误获取指标 {metric_name} for {instance_id}: {e}")
        return []

def calculate_write_io_stats(write_io_data):
    """计算写IO统计"""
    if not write_io_data:
        return 0, 0
    
    total_write_io = sum(write_io_data)
    avg_daily_write_io = total_write_io / len(write_io_data) if write_io_data else 0
    
    return int(total_write_io), int(avg_daily_write_io)

def calculate_storage_growth(free_storage_data):
    """计算存储增长（通过空闲存储空间变化估算）"""
    if len(free_storage_data) < 2:
        return "N/A", "N/A"
    
    # 转换为GB
    storage_gb = [bytes_val / (1024**3) for bytes_val in free_storage_data]
    
    # 计算增长量（空闲空间减少 = 数据增长）
    first_free = max(storage_gb)  # 最大空闲空间
    last_free = min(storage_gb)   # 最小空闲空间
    
    data_growth = first_free - last_free if first_free > last_free else 0
    avg_daily_growth = data_growth / 30
    
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
    end_time = datetime.utcnow()
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
        print(f"分析集群: {cluster_id} ({cluster['engine']})")
        
        # 获取集群实例
        instances = get_cluster_instances(rds_client, cluster_id)
        
        if not instances:
            print(f"  集群 {cluster_id} 中未找到实例")
            continue
        
        for instance in instances:
            instance_id = instance['identifier']
            print(f"  分析实例: {instance_id}")
            
            # 获取写IO数据
            print("    查询写IO数据...")
            write_io_data = get_metric_data(
                cloudwatch_client, 'WriteIOPS', instance_id, 
                start_time, end_time, 'Sum'
            )
            
            total_write_io, avg_daily_write_io = calculate_write_io_stats(write_io_data)
            
            # 获取存储数据
            print("    查询存储使用数据...")
            free_storage_data = get_metric_data(
                cloudwatch_client, 'FreeStorageSpace', instance_id,
                start_time, end_time, 'Average'
            )
            
            data_growth, avg_daily_growth = calculate_storage_growth(free_storage_data)
            
            # 保存结果
            result = {
                '集群名称': cluster_id,
                '实例ID': instance_id,
                '引擎': instance['engine'],
                '实例类型': instance['instance_class'],
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
        print("1. 数据增长量通过FreeStorageSpace变化估算，可能不够精确")
        print("2. 建议结合Aurora的实际存储使用量进行验证")
        print("3. Global Database的跨区域复制会产生额外的写IO成本")
        print("4. 可以使用AWS Pricing Calculator进一步估算成本: https://calculator.aws")
        
        # 显示前几行数据
        print()
        print("数据预览:")
        print("-" * 100)
        for i, result in enumerate(results[:5]):
            print(f"{i+1}. {result['集群名称']}/{result['实例ID']} - "
                  f"写IO: {result['30天总写IO次数']:,}, "
                  f"数据增长: {result['30天数据增长(GB)']}GB")
        
        if len(results) > 5:
            print(f"... 还有 {len(results) - 5} 个实例")
    
    else:
        print("未找到任何Aurora实例数据")

if __name__ == "__main__":
    main()
