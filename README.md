# 特殊连锁当天数据校验功能

## 功能概述

在 `table_data_vatify.py` 中新增了特殊连锁的当天数据校验功能。该功能专门针对指定的连锁ID，校验其 MongoDB 中 `create_time` 字段是否等于当前日期。

## 新增配置

### 配置文件修改

在 `mongodb_report.conf` 文件的 `[mongodb]` 部分添加新字段：

```ini
[mongodb]
serverHost = your.mongodb.host
serverPort = 2210
mongoUser = your_username
mongoPass = your_password
authDb = admin
databaseName = your_database
collections = order_c,order_m
chainIds = 1374766312710033408,1367089949295333376,1359536475405897728
# 需要特殊校验当天数据的连锁ID（校验create_time是否等于当前日期）
special_validation_chain_id = 1374766312710033408
```

### 配置说明

- **`special_validation_chain_id`**: 需要进行特殊当天数据校验的连锁ID
- 该字段为可选，如果不配置或为空，则跳过特殊校验
- 只能配置一个连锁ID进行特殊校验

## 新增功能

### 1. 核心校验函数

#### `validate_special_chain_today_data(client, config)`
- **功能**：校验特殊连锁的当天数据
- **校验逻辑**：
  - 检查指定连锁在所有配置集合中的当天数据
  - 验证 `create_time` 字段是否在当天时间范围内
  - 验证最新数据的创建时间是否为当天
- **返回**：详细的校验结果字典

#### `format_special_validation_message(special_result, config)`
- **功能**：格式化特殊校验结果为企业微信消息
- **返回**：企业微信 Markdown 格式消息

### 2. 校验逻辑

#### 时间范围定义
```python
# 当天时间范围（CST时区）
today_start = now_cst.replace(hour=0, minute=0, second=0, microsecond=0)
today_end = now_cst.replace(hour=23, minute=59, second=59, microsecond=999999)
```

#### 校验条件
对于特殊连锁的每个集合，需要同时满足：
- ✅ **当天有数据**：`today_count > 0`
- ✅ **最新数据是当天的**：`is_latest_today = True`

#### 整体校验结果
- ✅ **通过**：所有集合的校验都通过
- ❌ **失败**：任何一个集合的校验失败

### 3. 集成到主程序

在 `generate_report()` 函数中的第 17.5 步添加了特殊校验：

```python
# 17.5. 执行特殊连锁的当天数据校验
logger.info("开始执行特殊连锁的当天数据校验")
special_validation_result = validate_special_chain_today_data(client, config)

# 保存特殊校验结果到文件
special_validation_file = f"{directory}/special_validation_{today}.json"
```

## 校验结果格式

### 成功结果示例
```json
{
  "enabled": true,
  "success": true,
  "chain_id": "1374766312710033408",
  "chain_name": "海南华健医药有限公司",
  "total_collections": 2,
  "successful_collections": 2,
  "failed_collections": 0,
  "validation_results": [
    {
      "collection": "order_c",
      "success": true,
      "today_count": 150,
      "total_count": 50000,
      "latest_create_time": "2024-01-15 14:30:25",
      "is_latest_today": true
    },
    {
      "collection": "order_m",
      "success": true,
      "today_count": 80,
      "total_count": 30000,
      "latest_create_time": "2024-01-15 15:20:10",
      "is_latest_today": true
    }
  ],
  "validation_time": "2024-01-15 15:30:00",
  "today_date": "2024-01-15"
}
```

### 失败结果示例
```json
{
  "enabled": true,
  "success": false,
  "chain_id": "1374766312710033408",
  "chain_name": "海南华健医药有限公司",
  "total_collections": 2,
  "successful_collections": 1,
  "failed_collections": 1,
  "validation_results": [
    {
      "collection": "order_c",
      "success": true,
      "today_count": 150,
      "total_count": 50000,
      "latest_create_time": "2024-01-15 14:30:25",
      "is_latest_today": true
    },
    {
      "collection": "order_m",
      "success": false,
      "today_count": 0,
      "total_count": 30000,
      "latest_create_time": "2024-01-14 23:45:10",
      "is_latest_today": false
    }
  ],
  "validation_time": "2024-01-15 15:30:00",
  "today_date": "2024-01-15"
}
```

## 企业微信通知

### 通知内容
- 📊 **连锁信息**：连锁名称和校验状态
- 📋 **详细结果**：每个集合的校验结果
- ⚠️ **异常信息**：失败的详细原因

### 成功通知示例
```markdown
# 📊 海南华健医药有限公司 数据统计报告
**统计日期**: 2025-08-01
**总记录数**: 11636

## ✅ 数据状态
所有数据均为最新，无异常
```

### 失败通知示例
```markdown
# 📊 海南华健医药有限公司 数据统计报告
**统计日期**: 2025-08-01
**总记录数**: 8520

## ⚠️ 异常数据
以下数据需要关注:

- **主订单表**: 无当天数据
- **子订单表**: 最新数据非当天
```

## 输出文件

### 1. 特殊校验结果文件
- `mongo_reports/special_validation_YYYYMMDD.json` - 特殊校验详细结果

### 2. 日志记录
```
2024-01-15 15:30:01 - INFO - 开始执行特殊连锁的当天数据校验
2024-01-15 15:30:02 - INFO - 开始特殊校验连锁 1374766312710033408 的当天数据
2024-01-15 15:30:03 - INFO -   校验集合: order_c
2024-01-15 15:30:04 - INFO -   ✅ 特殊校验通过: order_c 有 150 条当天数据
2024-01-15 15:30:05 - INFO -   校验集合: order_m
2024-01-15 15:30:06 - WARNING -   ⚠️ 特殊校验失败: order_m 没有当天数据
2024-01-15 15:30:07 - WARNING - ⚠️ 特殊连锁 海南华健医药有限公司 有 1 个集合的当天数据校验失败
```

## 使用方法

### 1. 配置设置
1. 编辑 `mongodb_report.conf` 文件
2. 在 `[mongodb]` 部分添加 `special_validation_chain_id` 字段
3. 设置需要特殊校验的连锁ID

### 2. 运行程序
```bash
python table_data_vatify.py
```

程序会自动：
1. 执行常规的数据统计报告
2. 执行特殊连锁的当天数据校验
3. 发送企业微信通知（包含特殊校验结果）
4. 保存详细的校验结果文件

### 3. 禁用特殊校验
如果不需要特殊校验，可以：
- 删除 `special_validation_chain_id` 配置项
- 或将其设置为空值：`special_validation_chain_id = `

## 与现有功能的关系

### 1. 独立性
- 特殊校验功能独立于现有的数据统计功能
- 不影响原有的报告生成逻辑
- 可以单独启用或禁用

### 2. 互补性
- 现有功能：统计所有连锁的数据量和最新更新时间
- 特殊校验：专门校验指定连锁的当天数据完整性
- 两者结合提供更全面的数据质量监控

### 3. 通知整合
- 特殊校验结果会作为独立消息发送到企业微信
- 与现有的连锁报告消息分开发送
- 避免消息过长和混淆

## 故障排除

### 1. 配置问题
- **问题**：特殊校验被跳过
- **原因**：`special_validation_chain_id` 未配置或为空
- **解决**：检查配置文件中的字段设置

### 2. 连锁ID格式问题
- **问题**：提示"无效的链ID格式"
- **原因**：连锁ID不是有效的数字
- **解决**：确保连锁ID为纯数字格式

### 3. 数据查询问题
- **问题**：查询结果异常
- **原因**：时区处理或字段名称问题
- **解决**：检查 `create_time` 字段是否存在，确认时区设置

## 扩展功能

### 1. 多连锁支持
可以扩展支持多个连锁的特殊校验：
```ini
special_validation_chain_ids = 1374766312710033408,1367089949295333376
```

### 2. 自定义校验规则
可以添加更多校验条件：
- 数据量阈值检查
- 时间间隔检查
- 数据质量检查

### 3. 告警级别
可以设置不同的告警级别：
- 警告：部分集合失败
- 严重：所有集合失败
- 致命：连接或配置错误

这个功能为特定连锁提供了更精确的数据质量监控，确保关键业务数据的及时性和完整性。
