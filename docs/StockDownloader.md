# StockDownloader 模块文档

## 模块概述

StockDownloader 是项目的核心数据获取模块，负责从各种金融数据源获取股票、指数和ETF的数据，并将其存储到数据库中。

## 主要功能

1. **数据下载**
   - 股票日线数据下载
   - 指数日线数据下载
   - ETF数据下载
   - 股票和指数基本信息更新

2. **数据更新**
   - 增量更新最新交易日数据
   - 历史数据补全
   - 定时自动更新

3. **API服务**
   - 提供RESTful API接口
   - 支持数据查询和下载

## 目录结构

```
StockDownloader/
├── src/
│   ├── api/          # API接口定义
│   ├── core/         # 核心配置和工具
│   ├── database/     # 数据库相关代码
│   ├── services/     # 业务服务层
│   ├── tasks/        # 任务定义
│   └── utils/        # 工具函数
├── cache/            # 数据缓存目录
├── requirements.txt  # 依赖包列表
└── Dockerfile.stockdownloader  # Docker配置文件
```

## 配置说明

### 环境变量配置

在 `.env` 文件中配置以下参数：

```env
DB_USER=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stock_db
```

## 运行模式

StockDownloader 支持多种运行模式：

1. 指数数据下载模式 (--mode 1)
2. 股票数据下载模式 (--mode 2)
3. 指数数据更新模式 (--mode 3)
4. 股票数据更新模式 (--mode 4)
5. 全量数据下载模式 (--mode 5)
6. 全量数据更新模式 (--mode 6)
7. 基本信息更新模式 (--mode 7)
8. 历史数据补全模式 (--mode 8)

## API接口

### 主要接口

- `/api/stocks`: 获取股票列表
- `/api/indices`: 获取指数列表
- `/api/stock/{code}/daily`: 获取股票日线数据
- `/api/index/{code}/daily`: 获取指数日线数据

## 异常处理

系统实现了完善的异常处理机制：

1. 网络请求重试
2. 数据下载失败重试
3. API限流处理
4. 数据验证和错误日志

## 日志系统

- 日志文件位置：`logs/`目录
- 日志级别：INFO、WARNING、ERROR
- 日志格式：时间 - 模块名 - 日志级别 - 消息

## 定时任务

系统包含以下定时任务：

1. 每日数据更新
2. 交易日股票列表更新
3. 定期数据完整性检查

## 开发指南

### 添加新数据源

1. 在 `services/data_fetcher.py` 中添加新的数据获取方法
2. 在 `services/data_saver.py` 中添加对应的数据保存方法
3. 在 `tasks/` 目录下创建新的任务处理模块

### 添加新API接口

1. 在 `api/` 目录下添加新的路由
2. 实现对应的处理函数
3. 在 `api/app.py` 中注册新路由

## 性能优化

1. 使用多线程处理并发下载
2. 实现数据缓存机制
3. 采用批量数据库操作
4. 优化API响应速度

## 注意事项

1. 遵守API使用限制
2. 定期备份数据库
3. 监控系统资源使用
4. 及时处理错误日志