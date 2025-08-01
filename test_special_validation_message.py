#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试特殊校验消息格式
"""

from datetime import datetime

def format_special_validation_message(special_result, config):
    """
    格式化特殊校验结果为企业微信消息
    """
    try:
        if not special_result.get('enabled', False):
            return None
        
        chain_name = special_result.get('chain_name', '未知连锁')
        success = special_result.get('success', False)
        today_date = special_result.get('today_date', datetime.now().strftime('%Y-%m-%d'))
        validation_results = special_result.get('validation_results', [])
        
        # 计算总记录数
        total_records = sum(result.get('today_count', 0) for result in validation_results if result.get('success', False))
        
        # 构建消息内容 - 使用新的样式
        markdown_content = f"""# 📊 {chain_name} 数据统计报告
**统计日期**: {today_date}  
**总记录数**: {total_records}  
"""
        
        # 根据校验结果添加状态信息
        if success:
            markdown_content += """
## ✅ 数据状态
所有数据均为最新，无异常"""
        else:
            # 如果有失败的校验，显示异常信息
            failed_results = [r for r in validation_results if not r.get('success', False)]
            if failed_results:
                markdown_content += "\n## ⚠️ 异常数据\n"
                markdown_content += "以下数据需要关注:\n\n"
                
                collection_mappings = config.get('collection_mappings', {})
                
                for result in failed_results:
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
                        problem = "数据异常"
                    
                    markdown_content += f"- **{display_collection}**: {problem}\n"
            else:
                markdown_content += "\n## ✅ 数据状态\n所有数据均为最新，无异常"
        
        # 添加系统级错误信息（如果有）
        if 'error' in special_result:
            markdown_content += f"\n## ❌ 系统错误\n{special_result['error']}"
        
        return {
            "msgtype": "markdown",
            "markdown": {
                "content": markdown_content
            },
            "mentioned_list": config.get('wechat', {}).get('mentioned_list', []),
            "mentioned_mobile_list": config.get('wechat', {}).get('mentioned_mobile_list', [])
        }
        
    except Exception as e:
        print(f"❌ 格式化特殊校验消息时出错: {str(e)}")
        return None


def test_success_message():
    """测试成功消息"""
    print("=== 测试成功消息 ===")
    
    special_result = {
        'enabled': True,
        'success': True,
        'chain_name': '海南华健医药有限公司',
        'today_date': '2025-08-01',
        'validation_results': [
            {
                'collection': 'order_c',
                'success': True,
                'today_count': 6500,
                'is_latest_today': True
            },
            {
                'collection': 'order_m',
                'success': True,
                'today_count': 5136,
                'is_latest_today': True
            }
        ]
    }
    
    config = {
        'collection_mappings': {
            'order_c': '子订单表',
            'order_m': '主订单表'
        },
        'wechat': {
            'mentioned_list': ['肖磊', '尤明东(明东)'],
            'mentioned_mobile_list': ['13800000000']
        }
    }
    
    message = format_special_validation_message(special_result, config)
    if message:
        print(message['markdown']['content'])
    else:
        print("消息生成失败")


def test_failure_message():
    """测试失败消息"""
    print("\n=== 测试失败消息 ===")
    
    special_result = {
        'enabled': True,
        'success': False,
        'chain_name': '海南华健医药有限公司',
        'today_date': '2025-08-01',
        'validation_results': [
            {
                'collection': 'order_c',
                'success': True,
                'today_count': 6500,
                'is_latest_today': True
            },
            {
                'collection': 'order_m',
                'success': False,
                'today_count': 0,
                'is_latest_today': False,
                'latest_create_time': '2025-07-31 23:45:10'
            }
        ]
    }
    
    config = {
        'collection_mappings': {
            'order_c': '子订单表',
            'order_m': '主订单表'
        },
        'wechat': {
            'mentioned_list': ['肖磊', '尤明东(明东)'],
            'mentioned_mobile_list': ['13800000000']
        }
    }
    
    message = format_special_validation_message(special_result, config)
    if message:
        print(message['markdown']['content'])
    else:
        print("消息生成失败")


def test_system_error_message():
    """测试系统错误消息"""
    print("\n=== 测试系统错误消息 ===")
    
    special_result = {
        'enabled': True,
        'success': False,
        'chain_name': '海南华健医药有限公司',
        'today_date': '2025-08-01',
        'error': '数据库连接超时',
        'validation_results': []
    }
    
    config = {
        'collection_mappings': {},
        'wechat': {
            'mentioned_list': ['肖磊', '尤明东(明东)'],
            'mentioned_mobile_list': ['13800000000']
        }
    }
    
    message = format_special_validation_message(special_result, config)
    if message:
        print(message['markdown']['content'])
    else:
        print("消息生成失败")


if __name__ == "__main__":
    test_success_message()
    test_failure_message()
    test_system_error_message()
