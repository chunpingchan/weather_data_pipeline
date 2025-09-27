# 天气数据管道项目

基于Docker容器化的和风天气数据管道，用于采集、处理和可视化天气数据，数据采集来自于和风API。

## 功能特性

- 🌤️ 多城市天气数据实时采集
- ⚙️ 基于 Apache Airflow 的工作流调度
- 🐘 PostgreSQL 数据存储
- 📊 Grafana 数据可视化
- 🐳 全容器化部署

## 快速开始

### 环境要求

- Docker 28.4+
- Docker Compose 2.39+
- Git 2.51+

### 安装步骤

1. 克隆项目：
```bash
git clone https://github.com/chunpingchan/weather_data_pipeline.git
cd weather-data-pipeline
```

2. 配置环境变量
```bash
cp .env.example .env
# 编辑 .env 文件，填入你的配置
```

3. 启动服务
```bash
docker-compose up -d
```

4. 访问服务
- Airflow: http://localhost:8080 (admin/admin)
- Grafana: http://localhost:3000 (admin/admin)

## 开发指南

### 项目结构

```text
weather-data-pipeline/
├── airflow/          # Airflow 配置和 DAG
├── config/           # 配置文件
├── scripts/          # Python 数据处理脚本
├── grafana/          # Grafana 配置
├── .git/             # GitHub Actions 配置
├── .env              # 环境变量配置，通过.env.example配置
├── init.sql          # 数据库初始化报表
├── .gitignore
├── docker-compose.yml
└── README.md
```