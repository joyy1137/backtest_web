# 回测系统 (Backtest Web)

这是一个用于投资组合回测和结果导入的混合系统，结合了 MATLAB 回测引擎和 Python 数据导入工具。

## 项目概述

本系统主要用于：
- 执行投资组合回测分析
- 计算组合净值、贡献度、性能指标等
- 将回测结果自动导入 MySQL 数据库
- 生成回测报告和可视化图表

## 项目结构

```
backtest_web/
├── config/                    # 配置文件目录
│   ├── db.yaml               # 数据库连接配置
│   └── paths.yaml            # 路径配置
├── Optimizer_matlab/         # MATLAB 回测核心模块
│   ├── +BacktestToolbox/     # 回测工具箱类
│   ├── tools/                # 工具函数（数据库连接、工作日计算等）
│   ├── utils/                # 工具函数（数据处理、输出生成等）
│   └── config/               # MATLAB 配置文件
├── output/                   # 回测结果输出目录
│   └── backtest_results/     # 回测结果文件
├── logs/                     # 日志文件目录
├── import_*.py               # Python 数据导入脚本
├── importer.py               # MySQL 导入工具类
├── run_backtest.m            # 主回测脚本
└── main_history_web.m        # 历史回测主入口

```

## 功能模块

### 1. MATLAB 回测模块
- **BacktestToolbox**: 回测工具箱，提供完整的回测功能
  - 计算组合净值
  - 计算基准净值
  - 计算每日贡献度
  - 计算换手率
  - 生成回测报告和图表

### 2. Python 数据导入模块
- **import_netvalue_to_mysql.py**: 导入净值数据到 MySQL
- **import_performance_to_mysql.py**: 导入性能摘要数据到 MySQL
- **import_contributions_to_mysql.py**: 导入贡献度和权重数据到 MySQL
- **importer.py**: MySQL 导入工具类，提供数据导入和表管理功能

## 环境要求

### Python 环境
- Python 3.12
- 所需 Python 包见 `requirements.txt`

### MATLAB 环境
- MATLAB R2025a 或更高版本
- YAMLMatlab 工具箱（用于读取 YAML 配置文件）
- MySQL JDBC 驱动（mysql-connector-j-9.3.0.jar）

## 安装步骤

### 1. 安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 2. 配置数据库连接

编辑 `config/db.yaml`，配置 MySQL 数据库连接信息：


### 3. 配置路径

编辑 `config/paths.yaml`，配置相关路径：


### 4. 配置 MATLAB 工具箱

确保 YAMLMatlab 工具箱已正确安装并添加到 MATLAB 路径中。

## 使用方法

### 运行回测

在 MATLAB 中执行：

```matlab
run_backtest
```


## 输出文件说明

回测完成后，会在 `output/backtest_results/<user_name>/<id>/<session_id>/<portfolio_name>_回测<start_date>_to_<end_date>/` 目录下生成：

- `*_回测.csv`: 回测净值数据
- `*_contribution.csv`: 贡献度数据
- `*_contribution_weight.csv`: 权重贡献度数据
- `*_performance_summary.csv`: 性能摘要数据
- `*_组合基准对比图.png`: 组合与基准对比图
- `*_贡献分析对比图.png`: 贡献分析图
- `*_超额净值图.png`: 超额净值图
- `*回测分析报告_*.pdf`: 回测分析报告

## 数据库表结构

### 净值表 (`*_backtest`)
- `valuation_date`: 估值日期
- `portfolio_name`: 组合名称
- `benchmark_net_value`: 基准净值
- `portfolio_net_value`: 组合净值
- `excess_net_value`: 超额净值
- `session_id`: 会话ID
- `id`: 组合ID
- `update_time`: 更新时间

### 性能摘要表 (`*_backtest_performance`)
- `annual_return_pct`: 年化收益率
- `sharpe_ratio`: 夏普比率
- `info_ratio`: 信息比率
- `max_drawdown_pct`: 最大回撤
- `annual_vol_pct`: 年化波动率
- `portfolio_name`: 组合名称
- `session_id`: 会话ID
- `id`: 组合ID
- `update_time`: 更新时间

### 贡献度表 (`*_contribution`)
- 包含每日各股票的贡献度数据

### 权重贡献度表 (`*_contribution_weight`)
- 包含每日各股票的权重贡献度数据

## 日志

系统日志文件保存在 `logs/` 目录下，文件命名格式为：
- `weight_optimizer_YYYYMMDD.log`

## 注意事项

1. 确保 MySQL 数据库已创建并配置正确
2. 确保所有路径配置正确，特别是 Windows 路径格式
3. 运行回测前，确保输入数据文件存在于配置的输入路径中
4. 确保有足够的磁盘空间存储回测结果

## 故障排除

### Python 导入失败
- 检查 Python 环境是否正确安装
- 检查 `requirements.txt` 中的依赖是否已安装
- 检查数据库连接配置是否正确

### MATLAB 回测失败
- 检查 MATLAB 版本是否满足要求
- 检查 YAMLMatlab 工具箱是否正确安装
- 检查输入数据文件是否存在
- 查看日志文件获取详细错误信息

### 数据库连接失败
- 检查 `config/db.yaml` 中的连接信息
- 确保数据库服务正在运行
- 检查网络连接和防火墙设置


