# 使用官方Airflow镜像作为基础
FROM apache/airflow:2.5.1

# 切换到root用户安装系统依赖
USER root

# 安装系统依赖
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
		python3 \
        python3-dev \
        libpq-dev \
        curl \
        vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 创建日志目录并设置权限
RUN set -eux; \
    (getent group ${AIRFLOW_GID:-50000} || groupadd -g ${AIRFLOW_GID:-50000} airflow) && \
    (id -u ${AIRFLOW_UID:-50000} || useradd -u ${AIRFLOW_UID:-50000} -g airflow airflow) && \
    mkdir -p /opt/airflow/logs && \
    chown -R ${AIRFLOW_UID:-50000}:${AIRFLOW_GID:-50000} /opt/airflow/logs && \
    ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

# 切换回airflow用户
USER airflow

# 复制requirements文件
COPY requirements.txt .

# 安装Python依赖，使用清华镜像加速
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 设置环境变量
ENV PYTHONPATH=/opt/airflow/scripts
ENV AIRFLOW_HOME=/opt/airflow

# 设置工作目录
WORKDIR /opt/airflow

# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1