#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MongoDB 当天数据校验工具
专门用于校验某个连锁的MongoDB中create_time的时间为当天
"""

import os
import sys
import json
import traceback
from datetime import datetime

# 导入公共模块
from mongodb_common import (
    ConfigManager, MongoDBManager, WeChatNotifier,
    TodayDataValidator, ValidationReportFormatter,
    setup_logger
)

# 设置日志
logger = setup_logger("TodayValidation", "today_validation.log")


# 删除重复的函数，使用公共模块中的实现


def validate_today_create_time(client, database_name, collection_name, chain_id, config):
    """
    校验某个连锁的MongoDB中create_time的时间为当天
    
    Args:
        client: MongoDB客户端
        database_name: 数据库名称
        collection_name: 集合名称
        chain_id: 连锁ID
        config: 配置信息
    
    Returns:
        dict: 验证结果
    """
    try:
        logger.info(f"校验连锁 {chain_id} 在集合 {collection_name} 中的当天数据")
        
        # 获取当前日期（CST时区）
        cst_tz = pytz.timezone('Asia/Shanghai')
        now_cst = datetime.now(cst_tz)
        today_start = now_cst.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = now_cst.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # 转换为UTC时间用于查询
        today_start_utc = today_start.astimezone(pytz.utc)
        today_end_utc = today_end.astimezone(pytz.utc)
        
        db = client[database_name]
        collection = db[collection_name]
        
        # 将chain_id转换为整数
        try:
            chain_id_long = int(chain_id)
        except ValueError:
            return {
                'success': False,
                'chain_id': chain_id,
                'collection': collection_name,
                'error': f"无效的链ID格式: {chain_id}",
                'today_count': 0,
                'total_count': 0,
                'validation_time': now_cst.strftime('%Y-%m-%d %H:%M:%S')
            }
        
        # 查询当天的数据
        today_query = {
            "chain_id": chain_id_long,
            "create_time": {
                "$gte": today_start_utc,
                "$lte": today_end_utc
            }
        }
        
        # 查询该连锁的总数据
        total_query = {"chain_id": chain_id_long}
        
        # 执行查询
        today_count = collection.count_documents(today_query)
        total_count = collection.count_documents(total_query)
        
        # 获取最新的create_time
        latest_doc = collection.find_one(
            {"chain_id": chain_id_long},
            projection=["create_time"],
            sort=[("create_time", DESCENDING)]
        )
        
        latest_create_time = None
        latest_create_time_str = "无数据"
        is_today = False
        
        if latest_doc and 'create_time' in latest_doc:
            latest_create_time = latest_doc['create_time']
            
            # 处理时区转换
            if isinstance(latest_create_time, datetime):
                if latest_create_time.tzinfo is None:
                    # 假设为UTC时间
                    latest_create_time = pytz.utc.localize(latest_create_time)
                
                latest_cst = latest_create_time.astimezone(cst_tz)
                latest_create_time_str = latest_cst.strftime('%Y-%m-%d %H:%M:%S')
                
                # 检查是否为当天
                latest_date = latest_cst.date()
                today_date = now_cst.date()
                is_today = (latest_date == today_date)
        
        # 获取连锁名称
        chain_mappings = config.get('chain_mappings', {})
        chain_name = chain_mappings.get(chain_id, f"连锁ID:{chain_id}")
        
        # 判断验证结果
        validation_success = (today_count > 0 and is_today)
        
        result = {
            'success': validation_success,
            'chain_id': chain_id,
            'chain_name': chain_name,
            'collection': collection_name,
            'today_count': today_count,
            'total_count': total_count,
            'latest_create_time': latest_create_time_str,
            'is_latest_today': is_today,
            'validation_time': now_cst.strftime('%Y-%m-%d %H:%M:%S'),
            'today_date': now_cst.strftime('%Y-%m-%d')
        }
        
        # 记录验证结果
        if validation_success:
            logger.info(f"✅ 验证通过: {chain_name} 在 {collection_name} 中有 {today_count} 条当天数据")
        else:
            if today_count == 0:
                logger.warning(f"⚠️ 验证失败: {chain_name} 在 {collection_name} 中没有当天数据")
            elif not is_today:
                logger.warning(f"⚠️ 验证失败: {chain_name} 在 {collection_name} 中最新数据不是当天 (最新: {latest_create_time_str})")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ 校验连锁 {chain_id} 当天数据时出错: {str(e)}")
        return {
            'success': False,
            'chain_id': chain_id,
            'collection': collection_name,
            'error': str(e),
            'today_count': 0,
            'total_count': 0,
            'validation_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


def validate_all_chains_today_data(client, config):
    """批量校验所有连锁的当天数据"""
    try:
        logger.info("开始批量校验所有连锁的当天数据")
        
        database_name = config['databaseName']
        collection_list = config['collections']
        chain_id_list = config['chainIds']
        
        all_results = []
        
        for collection_name in collection_list:
            logger.info(f"校验集合: {collection_name}")
            
            for chain_id in chain_id_list:
                result = validate_today_create_time(
                    client, database_name, collection_name, chain_id, config
                )
                all_results.append(result)
        
        # 统计结果
        total_validations = len(all_results)
        successful_validations = sum(1 for r in all_results if r['success'])
        failed_validations = total_validations - successful_validations
        
        logger.info(f"批量验证完成: 总计 {total_validations} 项, 成功 {successful_validations} 项, 失败 {failed_validations} 项")
        
        return all_results
        
    except Exception as e:
        logger.error(f"❌ 批量校验当天数据时出错: {str(e)}")
        return []


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


def format_validation_report(validation_results, config):
    """格式化验证报告为企业微信消息"""
    try:
        if not validation_results:
            return None
        
        # 统计结果
        total_validations = len(validation_results)
        successful_validations = sum(1 for r in validation_results if r['success'])
        failed_validations = total_validations - successful_validations
        
        # 获取当前时间
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        today_date = datetime.now().strftime('%Y-%m-%d')
        
        # 构建消息内容
        if failed_validations == 0:
            status_icon = "✅"
            status_text = "全部通过"
            color = "info"
        else:
            status_icon = "⚠️"
            status_text = "存在异常"
            color = "warning"
        
        markdown_content = f"""# {status_icon} MongoDB 当天数据校验报告
**校验日期**: {today_date}  
**校验时间**: {current_time}  
**校验状态**: <font color=\"{color}\">{status_text}</font>  
**成功**: <font color=\"info\">{successful_validations}</font>  
**失败**: <font color=\"warning\">{failed_validations}</font>  
**总计**: {total_validations}  

"""
        
        # 添加失败的验证详情
        failed_results = [r for r in validation_results if not r['success']]
        if failed_results:
            markdown_content += "## ⚠️ 异常详情\n"
            markdown_content += "| 连锁名称 | 集合 | 当天数据量 | 最新数据时间 | 问题描述 |\n"
            markdown_content += "|----------|------|------------|--------------|----------|\n"
            
            collection_mappings = config.get('collection_mappings', {})
            
            for result in failed_results:
                chain_name = result.get('chain_name', result.get('chain_id', '未知'))
                collection = result.get('collection', '未知')
                display_collection = collection_mappings.get(collection, collection)
                today_count = result.get('today_count', 0)
                latest_time = result.get('latest_create_time', '无数据')
                
                # 判断问题类型
                if 'error' in result:
                    problem = result['error']
                elif today_count == 0:
                    problem = "无当天数据"
                elif not result.get('is_latest_today', False):
                    problem = "最新数据非当天"
                else:
                    problem = "未知问题"
                
                markdown_content += f"| {chain_name} | {display_collection} | {today_count} | {latest_time} | {problem} |\n"
        else:
            markdown_content += "## ✅ 验证结果\n所有连锁的当天数据均正常\n"
        
        return {
            "msgtype": "markdown",
            "markdown": {
                "content": markdown_content
            },
            "mentioned_list": config['wechat'].get('mentioned_list', []),
            "mentioned_mobile_list": config['wechat'].get('mentioned_mobile_list', [])
        }
        
    except Exception as e:
        logger.error(f"❌ 格式化验证报告时出错: {str(e)}")
        return None


def main():
    """主函数"""
    try:
        logger.info("开始 MongoDB 当天数据校验")
        
        # 加载配置
        config = load_config()
        
        # MongoDB连接字符串
        connection_string = (
            f"mongodb://{config['mongoUser']}:{config['mongoPass']}@"
            f"{config['serverHost']}:{config['serverPort']}/"
            f"?authSource={config['authDb']}&retryWrites=true&w=majority"
        )
        
        # 连接到MongoDB
        logger.info(f"连接到 MongoDB 服务器: {config['serverHost']}:{config['serverPort']}")
        
        try:
            client = MongoClient(
                connection_string,
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=30000
            )
            
            # 检查连接
            client.server_info()
            logger.info(f"✓ 成功连接到数据库: {config['databaseName']}")
            
            # 执行当天数据校验
            validation_results = validate_all_chains_today_data(client, config)
            
            # 保存验证结果到文件
            if validation_results:
                today = datetime.now().strftime('%Y%m%d')
                directory = 'validation_reports'
                os.makedirs(directory, exist_ok=True)
                
                validation_file = f"{directory}/today_validation_{today}.json"
                try:
                    with open(validation_file, 'w', encoding='utf-8') as f:
                        json.dump(validation_results, f, ensure_ascii=False, indent=2, default=str)
                    logger.info(f"✓ 验证结果已保存到: {validation_file}")
                except Exception as e:
                    logger.error(f"保存验证结果失败: {str(e)}")
            
            # 发送企业微信通知
            wechat_config = config.get('wechat', {})
            if wechat_config.get('webhook') and validation_results:
                logger.info("发送当天数据校验报告...")
                validation_message = format_validation_report(validation_results, config)
                if validation_message:
                    send_wechat_notification(wechat_config['webhook'], validation_message)
            
            # 打印摘要
            if validation_results:
                total_validations = len(validation_results)
                successful_validations = sum(1 for r in validation_results if r['success'])
                failed_validations = total_validations - successful_validations
                
                print(f"\n{'='*50}")
                print("MongoDB 当天数据校验摘要")
                print(f"{'='*50}")
                print(f"校验时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"总计: {total_validations} 项")
                print(f"成功: {successful_validations} 项")
                print(f"失败: {failed_validations} 项")
                print(f"{'='*50}")
                
                if failed_validations > 0:
                    print("\n异常详情:")
                    for result in validation_results:
                        if not result['success']:
                            chain_name = result.get('chain_name', result.get('chain_id', '未知'))
                            collection = result.get('collection', '未知')
                            print(f"❌ {chain_name} - {collection}: {result.get('error', '数据异常')}")
            
            return True
            
        except PyMongoError as me:
            logger.error(f"❌ MongoDB 连接错误: {str(me)}")
            return False
        finally:
            # 确保关闭连接
            if 'client' in locals():
                client.close()
                logger.info("MongoDB 连接已关闭")
        
    except Exception as e:
        logger.error(f"❌ 主程序错误: {str(e)}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"程序意外终止: {str(e)}")
        sys.exit(1)
