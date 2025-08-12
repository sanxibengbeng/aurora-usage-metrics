#!/usr/bin/env python3
"""
Aurora Global Database 成本分析脚本
查询过去30天的Write IO和数据增长量
支持Aurora集群和RDS实例
"""

import boto3
import json
import csv
from datetime import datetime, timedelta, timezone
import sys
import hashlib
from botocore.exceptions import ClientError, NoCredentialsError

def get_current_region():
    """获取当前region"""
    try:
        session = boto3.Session()
        return session.region_name
    except Exception:
        return None

def mask_identifier(identifier):
    """对数据库标识符进行脱敏处理"""
    if not identifier:
        return "N/A"
    
    # 使用SHA256哈希的前8位作为脱敏标识符
    hash_object = hashlib.sha256(identifier.encode())
    hash_hex = hash_object.hexdigest()
    return f"db-{hash_hex[:8]}"

def get_aurora_clusters(rds_client):
    """获取所有Aurora集群（支持分页）"""
    try:
        aurora_clusters = []
        paginator = rds_client.get_paginator('describe_db_clusters')
        
        for page in paginator.paginate():
            for cluster in page['DBClusters']:
                if cluster['Engine'] in ['aurora-mysql', 'aurora-postgresql']:
                    # 检查是否使用了Secret Manager
                    uses_secret_manager = bool(cluster.get('MasterUserSecret'))
                    secret_arn = cluster.get('MasterUserSecret', {}).get('SecretArn', '') if uses_secret_manager else ''
                    
                    aurora_clusters.append({
                        'identifier': cluster['DBClusterIdentifier'],
                        'masked_identifier': mask_identifier(cluster['DBClusterIdentifier']),
                        'engine': cluster['Engine'],
                        'engine_version': cluster['EngineVersion'],
                        'uses_secret_manager': uses_secret_manager,
                        'secret_arn': secret_arn,
                        'type': 'Aurora集群'
                    })
        
        return aurora_clusters
    except ClientError as e:
        print(f"错误获取Aurora集群: {e}")
        return []

def get_rds_instances(rds_client):
    """获取所有RDS实例（非Aurora）"""
    try:
        rds_instances = []
        paginator = rds_client.get_paginator('describe_db_instances')
        
        for page in paginator.paginate():
            for instance in page['DBInstances']:
                # 只获取非Aurora的RDS实例（没有DBClusterIdentifier的实例）
                if not instance.get('DBClusterIdentifier'):
                    # 检查是否使用了Secret Manager
                    uses_secret_manager = bool(instance.get('MasterUserSecret'))
                    
                    rds_instances.append({
                        'identifier': instance['DBInstanceIdentifier'],
                        'masked_identifier': mask_identifier(instance['DBInstanceIdentifier']),
                        'engine': instance['Engine'],
                        'engine_version': instance['EngineVersion'],
                        'instance_class': instance['DBInstanceClass'],
                        'uses_secret_manager': uses_secret_manager,
                        'type': 'RDS实例'
                    })
        
        return rds_instances
    except ClientError as e:
        print(f"错误获取RDS实例: {e}")
        return []

def get_cluster_instances(rds_client, cluster_id):
    """获取集群中的实例（支持分页）"""
    try:
        instances = []
        paginator = rds_client.get_paginator('describe_db_instances')
        
        # 首先获取集群信息来获取成员角色信息
        cluster_info = rds_client.describe_db_clusters(DBClusterIdentifier=cluster_id)
        cluster_members = {}
        if cluster_info['DBClusters']:
            for member in cluster_info['DBClusters'][0].get('DBClusterMembers', []):
                cluster_members[member['DBInstanceIdentifier']] = "Writer" if member['IsClusterWriter'] else "Reader"
        
        for page in paginator.paginate():
            for instance in page['DBInstances']:
                if instance.get('DBClusterIdentifier') == cluster_id:
                    instance_id = instance['DBInstanceIdentifier']
                    instance_role = cluster_members.get(instance_id, "Unknown")
                    
                    instances.append({
                        'identifier': instance['DBInstanceIdentifier'],
                        'masked_identifier': mask_identifier(instance['DBInstanceIdentifier']),
                        'instance_class': instance['DBInstanceClass'],
                        'engine': instance['Engine'],
                        'role': instance_role
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
        return "N/A", "N/A", "N/A", "N/A"
    
    # volume_bytes_data 现在是 [(timestamp, value), ...] 的格式，已按时间排序
    # 获取月初（最早）和月末（最晚）的数据
    first_timestamp, first_value = volume_bytes_data[0]  # 月初
    last_timestamp, last_value = volume_bytes_data[-1]   # 月末
    
    # 转换为GB
    first_value_gb = first_value / (1024**3)
    last_value_gb = last_value / (1024**3)
    
    return int(first_value), round(first_value_gb, 2), int(last_value), round(last_value_gb, 2)

def main():
    print("Aurora & RDS 数据库成本分析脚本")
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
    
    # 获取Aurora集群和RDS实例
    print("正在获取数据库列表...")
    aurora_clusters = get_aurora_clusters(rds_client)
    rds_instances = get_rds_instances(rds_client)
    
    if not aurora_clusters and not rds_instances:
        print("未找到Aurora集群或RDS实例")
        sys.exit(0)
    
    print(f"找到 {len(aurora_clusters)} 个Aurora集群")
    print(f"找到 {len(rds_instances)} 个RDS实例")
    print()
    
    # 准备结果数据
    results = []
    
    # 分析Aurora集群
    for cluster in aurora_clusters:
        cluster_id = cluster['identifier']
        masked_cluster_id = cluster['masked_identifier']
        uses_secret_manager = cluster['uses_secret_manager']
        secret_arn = cluster['secret_arn']
        
        print(f"分析Aurora集群: {masked_cluster_id} ({cluster['engine']})")
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
        
        cluster_storage_result = calculate_storage_growth(cluster_volume_bytes_data)
        
        # 获取集群实例
        instances = get_cluster_instances(rds_client, cluster_id)
        
        if not instances:
            print(f"  集群 {masked_cluster_id} 中未找到实例")
            continue
        
        for instance in instances:
            instance_id = instance['identifier']
            masked_instance_id = instance['masked_identifier']
            instance_role = instance['role']
            print(f"  分析实例: {masked_instance_id} (角色: {instance_role})")
            
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
            start_bytes, start_gb, end_bytes, end_gb = cluster_storage_result
            
            # 保存结果
            result = {
                '数据库类型': cluster['type'],
                '集群/实例名称': masked_cluster_id,
                '实例ID': masked_instance_id,
                '实例角色': instance_role,
                '引擎': instance['engine'],
                '实例类型': instance['instance_class'],
                '使用Secret Manager': '是' if uses_secret_manager else '否',
                '30天总写IO次数': total_write_io,
                '月初的存储总量(原始值)': start_bytes,
                '月初的存储总量(GB)': start_gb,
                '月末存储总量(原始值)': end_bytes,
                '月末的存储总量(GB)': end_gb
            }
            
            results.append(result)
            
            print(f"    写IO总数: {total_write_io:,}, 平均每日: {avg_daily_write_io:,}")
            print(f"    月初存储: {start_gb}GB ({start_bytes} bytes)")
            print(f"    月末存储: {end_gb}GB ({end_bytes} bytes)")
            print()
    
    # 分析RDS实例
    for rds_instance in rds_instances:
        instance_id = rds_instance['identifier']
        masked_instance_id = rds_instance['masked_identifier']
        uses_secret_manager = rds_instance['uses_secret_manager']
        
        print(f"分析RDS实例: {masked_instance_id} ({rds_instance['engine']})")
        if uses_secret_manager:
            print(f"  使用Secret Manager: 是")
        else:
            print(f"  使用Secret Manager: 否")
        
        # 获取写IO数据
        print("  查询写IO数据...")
        write_io_data = get_metric_data(
            cloudwatch_client, 'WriteIOPS', instance_id, 
            start_time, end_time, 'Sum', period=3600  # 1小时间隔
        )
        
        total_write_io, avg_daily_write_io = calculate_write_io_stats(write_io_data)
        
        # 获取存储数据 - RDS实例使用DatabaseConnections或其他可用指标
        print("  查询存储使用数据...")
        # 对于RDS实例，我们尝试获取FreeStorageSpace来计算已用存储
        free_storage_data = get_metric_data(
            cloudwatch_client, 'FreeStorageSpace', instance_id,
            start_time, end_time, 'Average', period=86400  # 1天间隔
        )
        
        # 由于RDS实例的存储计算比较复杂，我们暂时设置为N/A
        start_bytes, start_gb, end_bytes, end_gb = "N/A", "N/A", "N/A", "N/A"
        
        # 保存结果
        result = {
            '数据库类型': rds_instance['type'],
            '集群/实例名称': masked_instance_id,
            '实例ID': masked_instance_id,
            '实例角色': 'Standalone',
            '引擎': rds_instance['engine'],
            '实例类型': rds_instance['instance_class'],
            '使用Secret Manager': '是' if uses_secret_manager else '否',
            '30天总写IO次数': total_write_io,
            '月初的存储总量(原始值)': start_bytes,
            '月初的存储总量(GB)': start_gb,
            '月末存储总量(原始值)': end_bytes,
            '月末的存储总量(GB)': end_gb
        }
        
        results.append(result)
        
        print(f"  写IO总数: {total_write_io:,}, 平均每日: {avg_daily_write_io:,}")
        print(f"  存储信息: RDS实例存储信息暂不支持")
        print()
    
    # 保存结果到CSV文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f'aurora_rds_cost_analysis_{timestamp}.csv'
    
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
        aurora_instances = len([r for r in results if r['数据库类型'] == 'Aurora集群'])
        rds_instances_count = len([r for r in results if r['数据库类型'] == 'RDS实例'])
        total_write_io_all = sum(r['30天总写IO次数'] for r in results)
        avg_daily_write_io_all = total_write_io_all // 30 if total_write_io_all > 0 else 0
        
        # 计算总存储增长（去重，因为同一集群的实例共享存储数据）
        unique_clusters = {}
        for r in results:
            if r['数据库类型'] == 'Aurora集群':
                cluster_name = r['集群/实例名称']
                if cluster_name not in unique_clusters:
                    start_gb = r['月初的存储总量(GB)']
                    end_gb = r['月末的存储总量(GB)']
                    if start_gb != 'N/A' and end_gb != 'N/A':
                        unique_clusters[cluster_name] = end_gb - start_gb
                    else:
                        unique_clusters[cluster_name] = 0
        
        total_storage_growth = sum(unique_clusters.values())
        
        print(f"总数据库实例数: {total_instances}")
        print(f"  - Aurora实例: {aurora_instances}")
        print(f"  - RDS实例: {rds_instances_count}")
        print(f"30天总写IO次数: {total_write_io_all:,}")
        print(f"平均每日写IO次数: {avg_daily_write_io_all:,}")
        print(f"Aurora总存储增长: {total_storage_growth:.2f} GB")
        
        print()
        print("注意事项:")
        print("1. 所有数据库标识符已脱敏处理")
        print("2. 写IO统计：Aurora使用VolumeWriteIOPS，RDS使用WriteIOPS指标")
        print("3. 存储增长：仅支持Aurora集群，RDS实例暂不支持")
        print("4. Global Database成本：跨区域复制会产生额外的写IO成本")
        print("5. 可以使用AWS Pricing Calculator进一步估算成本: https://calculator.aws")
        
        # 显示前几行数据
        print()
        print("数据预览:")
        print("-" * 120)
        for i, result in enumerate(results[:5]):
            secret_info = f"Secret: {result['使用Secret Manager']}"
            storage_info = f"存储: {result['月初的存储总量(GB)']}GB -> {result['月末的存储总量(GB)']}GB"
            role_info = f"角色: {result['实例角色']}"
            print(f"{i+1}. {result['数据库类型']} - {result['集群/实例名称']}/{result['实例ID']} - "
                  f"写IO: {result['30天总写IO次数']:,}, "
                  f"{storage_info}, "
                  f"{role_info}, "
                  f"{secret_info}")
        
        if len(results) > 5:
            print(f"... 还有 {len(results) - 5} 个实例")
    
    else:
        print("未找到任何数据库实例数据")

if __name__ == "__main__":
    main()
