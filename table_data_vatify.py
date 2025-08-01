#!/usr/bin/env python3
"""
MongoDB Report Generator with Per-Chain WeChat Notifications
"""

import os
import sys
import csv
import time
import json
import requests
import configparser
from datetime import datetime, timedelta
import pytz
import logging
from pymongo import MongoClient, DESCENDING
from pymongo.errors import PyMongoError
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mongodb_report.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MongoDBReport")


def load_config(config_path="mongodb_report.conf"):
    """加载配置文件"""
    config = configparser.ConfigParser()

    # 尝试读取配置文件，如果不存在则创建默认配置
    if not os.path.exists(config_path):
        logger.error(f"⚠️ 配置文件 '{config_path}' 不存在，创建默认配置...")
        create_default_config(config_path)
        logger.info("请编辑配置文件后重新运行脚本。")
        sys.exit(1)

    config.read(config_path, encoding='utf-8')

    # 验证配置是否有效
    if 'mongodb' not in config:
        logger.error("配置文件中缺少 [mongodb] 部分")
        sys.exit(1)

    mongodb_config = config['mongodb']

    # 定义必需参数
    required_params = [
        'serverHost', 'mongoUser', 'mongoPass', 'authDb',
        'databaseName', 'collections', 'chainIds'
    ]

    # 检查缺失参数
    missing_params = [param for param in required_params if param not in mongodb_config]
    if missing_params:
        logger.error(f"配置文件中缺少必需的参数: {', '.join(missing_params)}")
        sys.exit(1)

    # 处理参数
    server_port = mongodb_config.get('serverPort', '2210')
    try:
        server_port = int(server_port)
    except ValueError:
        logger.error(f"无效的 serverPort: '{server_port}'. 必须是整数")
        sys.exit(1)

    # 处理chain_mappings
    chain_mappings = {}
    # 处理collection_mappings
    collection_mappings = {}

    if 'wechat' in config:
        chain_mappings_str = config['wechat'].get('chain_mappings', '')
        # 改进映射字符串处理逻辑
        for mapping in chain_mappings_str.split(','):
            mapping = mapping.strip()
            if mapping and ':' in mapping:
                try:
                    # 只分割第一个冒号
                    chain_id, chain_name = mapping.split(':', 1)
                    chain_mappings[chain_id.strip()] = chain_name.strip()
                except ValueError:
                    logger.warning(f"无法解析连锁映射: {mapping}")

        # 处理集合名称映射
        collection_mappings_str = config['wechat'].get('collection_mappings', '')
        for mapping in collection_mappings_str.split(','):
            mapping = mapping.strip()
            if mapping and ':' in mapping:
                try:
                    # 只分割第一个冒号
                    eng_name, chn_name = mapping.split(':', 1)
                    collection_mappings[eng_name.strip()] = chn_name.strip()
                except ValueError:
                    logger.warning(f"无法解析集合映射: {mapping}")

    # 可选的企业微信配置
    wechat_config = {}
    if 'wechat' in config:
        wechat_config = {
            'webhook': config['wechat'].get('webhook', ''),
            'mentioned_list': [item.strip() for item in config['wechat'].get('mentioned_list', '').split(',') if
                               item.strip()],
            'mentioned_mobile_list': [item.strip() for item in
                                      config['wechat'].get('mentioned_mobile_list', '').split(',') if item.strip()],
        }

    return {
        'serverHost': mongodb_config['serverHost'],
        'serverPort': server_port,
        'mongoUser': mongodb_config['mongoUser'],
        'mongoPass': mongodb_config['mongoPass'],
        'authDb': mongodb_config['authDb'],
        'databaseName': mongodb_config['databaseName'],
        'collections': [col.strip() for col in mongodb_config['collections'].split(',') if col.strip()],
        'chainIds': [cid.strip() for cid in mongodb_config['chainIds'].split(',') if cid.strip()],
        'chain_mappings': chain_mappings,
        'collection_mappings': collection_mappings,  # 新增集合名称映射
        'wechat': wechat_config
    }


def create_default_config(config_path):
    """创建默认配置文件"""
    config = configparser.ConfigParser()

    # MongoDB 配置部分
    config['mongodb'] = {
        'serverHost': 'your.mongodb.host',
        'serverPort': '2210',
        'mongoUser': 'your_username',
        'mongoPass': 'your_password',
        'authDb': 'admin',
        'databaseName': 'your_database',
        'collections': 'collection1,collection2',
        'chainIds': '1001,1002'
    }

    # 企业微信机器人配置
    config['wechat'] = {
        'webhook': 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=your_key',
        'mentioned_list': 'user1,user2',
        'mentioned_mobile_list': '13800000000,13900000000',
        'chain_mappings': '1001:连锁A;1002:连锁B',
        'collection_mappings': 'collection1:示例表1,collection2:示例表2'  # 新增默认集合映射
    }

    with open(config_path, 'w', encoding='utf-8') as f:
        config.write(f)

    logger.info(f"✓ 已创建默认配置文件 '{config_path}'。请编辑此文件后重新运行脚本。")


def send_wechat_notification(webhook, data):
    """发送企业微信机器人通知"""
    if not webhook:
        logger.warning("未配置企业微信机器人，跳过通知发送")
        return False

    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.post(webhook, data=json.dumps(data), headers=headers, timeout=10)

        if response.status_code == 200 and response.json().get('errcode') == 0:
            logger.info("✓ 企业微信通知发送成功")
            return True
        else:
            logger.error(f"企业微信通知发送失败: {response.text}")
            return False
    except Exception as e:
        logger.error(f"发送企业微信通知时出错: {str(e)}")
        return False


def format_chain_markdown_message(chain_id, chain_name, chain_data, anomalies, config, execution_time):
    """为单个连锁格式化企业微信Markdown消息"""
    # 获取集合名称映射
    collection_mappings = config.get('collection_mappings', {})

    # 获取当前日期和前一天的日期
    today_date = datetime.now().strftime('%Y-%m-%d')
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 计算连锁总记录数
    total_records = sum(item[3] for item in chain_data if isinstance(item[3], int))

    # 构建Markdown内容
    markdown_content = f"""# 📊 {chain_name} 数据统计报告
**统计日期**: {today_date}  
**总记录数**: {total_records}  
"""

    # 添加详细统计结果部分
    # 如果你需要这部分内容，可以取消注释
    # if chain_data:
    #     markdown_content += f"\n## 📋 详细统计结果\n"
    #     markdown_content += "| 表名称 | 统计数量 | 最后更新时间 |\n"
    #     markdown_content += "|--------|----------|--------------|\n"
    #
    #     for item in chain_data:
    #         if not isinstance(item[3], int):  # 跳过错误行
    #             continue
    #
    #         timestamp, collection_name, _, record_count, max_time = item
    #
    #         # 使用集合映射获取中文表名，如果没有映射则使用原始名称
    #         display_table = collection_mappings.get(collection_name, collection_name)
    #
    #         # 格式化时间显示
    #         if isinstance(max_time, datetime):
    #             formatted_time = max_time.strftime('%Y-%m-%d %H:%M:%S')
    #         else:
    #             formatted_time = str(max_time)[:19]  # 截断以防过长
    #
    #         # 高亮异常更新时间
    #         if isinstance(max_time, datetime):
    #             max_time_str = max_time.strftime('%Y-%m-%d')
    #             if max_time_str != yesterday_date:
    #                 time_display = f"<font color=\"warning\">{formatted_time}</font>"
    #             else:
    #                 time_display = formatted_time
    #         else:
    #             time_display = formatted_time
    #
    #         # 高亮显示记录数较多的数据
    #         if record_count > 1000:
    #             markdown_content += f"| {display_table} | <font color=\"warning\">{record_count}</font> | {time_display} |\n"
    #         else:
    #             markdown_content += f"| {display_table} | {record_count} | {time_display} |\n"

    # 添加异常数据部分 - 使用中文表名映射
    if anomalies:
        markdown_content += "\n## ⚠️ 异常数据\n"
        markdown_content += f"以下数据的最新更新时间不是前一天日期({yesterday_date})，需要关注:\n\n"
        markdown_content += "| 表名称 | 最后更新时间 |\n"
        markdown_content += "|--------|--------------|\n"

        for anomaly in anomalies:
            collection = anomaly['collection']
            max_time = anomaly['max_time']

            # 使用集合映射获取中文表名，如果没有映射则使用原始名称
            display_table = collection_mappings.get(collection, collection)
            markdown_content += f"| {display_table} | <font color=\"warning\">{max_time}</font> |\n"
    else:
        markdown_content += "\n## ✅ 数据状态\n所有数据均为最新，无异常\n"

    return {
        "msgtype": "markdown",
        "markdown": {
            "content": markdown_content
        },
        "mentioned_list": config['wechat'].get('mentioned_list', []),
        "mentioned_mobile_list": config['wechat'].get('mentioned_mobile_list', [])
    }


def generate_report(config):
    """生成报告的主要功能"""
    try:
        logger.info("Starting MongoDB Report Generator")

        # 1. 从配置中获取参数
        server_host = config['serverHost']
        server_port = config['serverPort']
        mongo_user = config['mongoUser']
        mongo_pass = config['mongoPass']
        auth_db = config['authDb']
        database_name = config['databaseName']
        collection_list = config['collections']
        chain_id_list = config['chainIds']
        chain_mappings = config['chain_mappings']
        collection_mappings = config['collection_mappings']  # 获取集合映射
        wechat_config = config['wechat']
        wechat_enabled = bool(wechat_config.get('webhook', ''))

        # 验证参数
        if not collection_list:
            raise ValueError("未指定有效的集合")
        if not chain_id_list:
            raise ValueError("未指定有效的链ID")

        logger.info(f"主机: {server_host}:{server_port}")
        logger.info(f"数据库: {database_name}")
        logger.info(f"集合: {', '.join(collection_list)}")
        logger.info(f"链ID: {', '.join(chain_id_list)}")
        logger.info(f"链ID名称映射数: {len(chain_mappings)}")
        logger.info(f"集合名称映射数: {len(collection_mappings)}")
        if wechat_enabled:
            logger.info("✓ 企业微信机器人已启用")

        # 2. 创建输出目录和文件
        today = datetime.now().strftime('%Y%m%d')
        directory = 'mongo_reports'
        os.makedirs(directory, exist_ok=True)
        filename = f"{directory}/mongodb_report_{today}.csv"

        # 3. 创建CSV文件并写入表头
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['timestamp', 'collection_name', 'chain_id', 'record_count', 'last_create_time'])

        # 4. MongoDB连接字符串
        connection_string = (
            f"mongodb://{mongo_user}:{mongo_pass}@{server_host}:{server_port}/"
            f"?authSource={auth_db}&retryWrites=true&w=majority"
        )

        # 5. 连接到MongoDB
        logger.info(f"连接到 MongoDB 服务器: {server_host}:{server_port}")
        start_time = time.time()

        processed_records = 0
        results = []

        try:
            client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=10000,  # 10秒超时
                connectTimeoutMS=30000  # 30秒连接超时
            )

            # 检查连接
            client.server_info()
            logger.info(f"✓ 成功连接到数据库: {database_name}")

            db = client[database_name]

            # 6. 处理每个集合
            for collection_name in collection_list:
                logger.info(f"\n处理集合: {collection_name}")
                collection = db[collection_name]

                # 获取集合文档总数
                try:
                    total_docs = collection.estimated_document_count()
                    logger.info(f"集合包含约 {total_docs:,} 个文档")
                except Exception as e:
                    logger.error(f"⚠️ 无法获取文档数量: {str(e)}")
                    total_docs = "未知"

                # 7. 处理每个连锁ID
                for chain_id in chain_id_list:
                    logger.info(f"  检查链ID: {chain_id}")

                    try:
                        # 将chain_id转换为整数
                        try:
                            chain_id_long = int(chain_id)
                        except ValueError:
                            error_msg = f"无效的链ID格式: {chain_id}. 必须是数字。"
                            logger.error(f"   ❌ {error_msg}")
                            raise ValueError(error_msg)

                        # 8. 查询最新create_time
                        latest_doc = collection.find_one(
                            {"chain_id": chain_id_long},
                            projection=["create_time"],
                            sort=[("create_time", DESCENDING)]
                        )

                        max_create_time = None
                        record_count = 0

                        # 9. 处理查询结果
                        if latest_doc and 'create_time' in latest_doc:
                            max_create_time = latest_doc['create_time']

                            # 10. 处理时区和小时取整 (CST = Asia/Shanghai)
                            try:
                                cst_tz = pytz.timezone('Asia/Shanghai')

                                # 确保是datetime对象
                                if not isinstance(max_create_time, datetime):
                                    # 尝试转换可能的类型
                                    if isinstance(max_create_time, (int, float)):
                                        # 时间戳格式
                                        max_create_time = datetime.fromtimestamp(max_create_time)
                                    else:
                                        # 尝试从字符串解析
                                        formats = [
                                            '%Y-%m-%dT%H:%M:%S.%fZ',
                                            '%Y-%m-%d %H:%M:%S',
                                            '%Y-%m-%dT%H:%M:%S'
                                        ]
                                        for fmt in formats:
                                            try:
                                                if isinstance(max_create_time, str):
                                                    max_create_time = datetime.strptime(max_create_time, fmt)
                                                    break
                                            except:
                                                continue

                                # 处理时区
                                if max_create_time.tzinfo is None:
                                    # 假设为UTC时间
                                    max_create_time = pytz.utc.localize(max_create_time)

                                max_time_cst = max_create_time.astimezone(cst_tz)

                                # 向下取整到整点
                                rounded_hour = max_time_cst.replace(
                                    minute=0, second=0, microsecond=0
                                )

                                # 11. 查询最近一小时的记录数量
                                query = {
                                    "chain_id": chain_id_long,
                                    "create_time": {"$gt": rounded_hour}
                                }

                                try:
                                    record_count = collection.count_documents(query)
                                except Exception as e:
                                    logger.error(f"   ⚠️ 统计文档错误: {str(e)}")
                                    record_count = 0

                                # 12. 格式化时间
                                hour_formatted = rounded_hour.strftime('%Y-%m-%d %H:%M:%S')
                                logger.info(f"   ✓ 找到 {record_count} 条记录 (从 {hour_formatted} 开始)")

                            except Exception as e:
                                logger.error(f"   ⚠️ 日期处理错误: {str(e)}")
                                max_create_time = None
                                record_count = 0
                        else:
                            max_create_time = None
                            record_count = 0
                            logger.info(f"   ⚠️ 未找到链ID={chain_id}的文档")

                            # 检查是否有其他文档包含这个chain_id
                            try:
                                count_with_chain_id = collection.count_documents({"chain_id": chain_id_long})
                                logger.info(f"   - 包含此链ID的文档数: {count_with_chain_id}")

                                # 查看一条文档的结构（如果存在）
                                if count_with_chain_id > 0:
                                    sample_doc = collection.find_one({"chain_id": chain_id_long})
                                    if sample_doc:
                                        logger.info(f"   - 文档字段: {list(sample_doc.keys())}")

                                        # 尝试多种可能的日期字段名
                                        possible_date_fields = ["create_time", "CreateTime", "createTime", "createdAt"]
                                        for field in possible_date_fields:
                                            if field in sample_doc:
                                                field_value = sample_doc[field]
                                                logger.info(
                                                    f"   - 找到日期字段 '{field}': {type(field_value).__name__}")
                            except Exception as e:
                                logger.error(f"   ⚠️ 诊断信息获取失败: {str(e)}")

                        # 13. 准备结果行
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        if max_create_time:
                            if isinstance(max_create_time, datetime):
                                max_time_str = max_create_time
                            else:
                                # 处理非日期时间对象
                                max_time_str = str(max_create_time)
                        else:
                            max_time_str = None

                        result_line = [timestamp, collection_name, chain_id, record_count, max_time_str]
                        results.append(result_line)
                        processed_records += 1

                        # 14. 实时写入文件
                        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(result_line)

                        # 记录处理结果（含连锁名称）
                        chain_name = chain_mappings.get(chain_id, f"连锁ID:{chain_id}")
                        logger.info(f"   ✓ 处理完成: {collection_name}.{chain_name} = {record_count} 条记录")

                    except Exception as e:
                        error_msg = f"处理链ID {chain_id} 时出错: {str(e)}"
                        logger.error(f"   ❌ {error_msg}")
                        traceback.print_exc()

                        error_line = [
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            collection_name,
                            chain_id,
                            f"ERROR: {str(e)}",
                            "ERROR"
                        ]
                        results.append(error_line)

                        with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerow(error_line)

            # 15. 计算执行时间
            execution_time = time.time() - start_time
            hours, remainder = divmod(execution_time, 3600)
            minutes, seconds = divmod(remainder, 60)
            readable_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

            # 16. 生成摘要
            summary = f"""
================================================
MongoDB 日报摘要
================================================
报告日期:      {datetime.now().strftime('%Y-%m-%d')}
执行时间:      {readable_time}
数据库:         {database_name}
集合数量:       {len(collection_list)}
链ID数量:       {len(chain_id_list)} 
处理记录数:    {len(results)}
输出文件:       {os.path.abspath(filename)}
================================================
"""
            logger.info(summary)

            # 17. 保存摘要到单独文件
            with open(f"{directory}/report_summary_{today}.txt", 'w', encoding='utf-8') as f:
                f.write(summary)

            # 18. 发送企业微信通知（每个连锁单独发送）
            if wechat_enabled:
                wechat_webhook = wechat_config.get('webhook', '')
                if wechat_webhook:
                    try:
                        # 按连锁分组数据
                        chain_data = {}
                        for item in results:
                            if not isinstance(item[3], int):  # 跳过错误行
                                continue

                            chain_id = item[2]
                            if chain_id not in chain_data:
                                chain_data[chain_id] = []
                            chain_data[chain_id].append(item)

                        # 获取当前日期和前一天的日期
                        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

                        # 为每个连锁生成并发送消息
                        for chain_id, items in chain_data.items():
                            chain_name = chain_mappings.get(chain_id, f"连锁ID:{chain_id}")

                            # 检测该连锁的异常数据
                            anomalies = []
                            for item in items:
                                if not isinstance(item[3], int):  # 跳过错误行
                                    continue

                                timestamp, collection_name, _, record_count, max_time = item

                                # 检测异常数据：max_time 不属于前一天日期
                                if isinstance(max_time, datetime):
                                    max_time_str = max_time.strftime('%Y-%m-%d')
                                    if max_time_str != yesterday_date:
                                        # 使用集合映射获取中文表名，如果没有映射则使用原始名称
                                        display_table = collection_mappings.get(collection_name, collection_name)
                                        anomalies.append({
                                            'chain_name': chain_name,
                                            'collection': display_table,  # 使用映射后的表名
                                            'max_time': max_time_str
                                        })
                                elif isinstance(max_time, str) and len(max_time) >= 10:
                                    # 处理字符串格式的时间
                                    date_part = max_time[:10]
                                    if date_part != yesterday_date:
                                        display_table = collection_mappings.get(collection_name, collection_name)
                                        anomalies.append({
                                            'chain_name': chain_name,
                                            'collection': display_table,  # 使用映射后的表名
                                            'max_time': date_part
                                        })

                            # 构建企业微信消息
                            wechat_message = format_chain_markdown_message(
                                chain_id, chain_name, items, anomalies, config, readable_time
                            )

                            # 发送通知
                            send_wechat_notification(wechat_webhook, wechat_message)

                            # 避免发送过快导致限流
                            time.sleep(1)

                    except Exception as e:
                        logger.error(f"发送企业微信通知失败: {str(e)}")

            return summary

        except PyMongoError as me:
            error_msg = f"MongoDB 连接错误: {str(me)}"
            logger.error(f"❌ {error_msg}")
            return f"Connection failed: {str(me)}"
        finally:
            # 确保关闭连接
            client.close()
            logger.info("MongoDB 连接已关闭")

    except Exception as e:
        error_msg = f"报告生成错误: {str(e)}"
        logger.error(f"❌ {error_msg}")
        traceback.print_exc()
        return f"Report generation failed: {str(e)}"


if __name__ == "__main__":
    try:
        # 加载配置
        config = load_config()

        # 生成报告
        result = generate_report(config)
        print(result)
    except Exception as e:
        logger.error(f"❌ 主程序错误: {str(e)}")
        traceback.print_exc()
        sys.exit(1)