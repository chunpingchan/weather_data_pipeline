-- 创建扩展（如果需要）
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 创建天气数据表
CREATE TABLE IF NOT EXISTS weather_now (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(50) NOT NULL,
    city_id VARCHAR(20) NOT NULL,
    update_time TIMESTAMP WITH TIME ZONE,
    temp NUMERIC(5, 2),
    feels_like NUMERIC(5, 2),
    text VARCHAR(50),
    wind_scale VARCHAR(10),
    humidity INTEGER,
    pressure INTEGER,
    vis INTEGER,
    cloud VARCHAR(10),
    insert_ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_weather_now_city ON weather_now(city_name);
CREATE INDEX IF NOT EXISTS idx_weather_now_timestamp ON weather_now(insert_ts);
CREATE INDEX IF NOT EXISTS idx_weather_now_city_timestamp ON weather_now(city_name, insert_ts);

-- 创建视图便于查询最新数据
CREATE OR REPLACE VIEW latest_weather AS
SELECT DISTINCT ON (city_name) *
FROM weather_now
ORDER BY city_name, insert_ts DESC;

-- 添加注释
COMMENT ON TABLE weather_now IS '存储实时天气数据';
COMMENT ON COLUMN weather_now.temp IS '温度，摄氏度';
COMMENT ON COLUMN weather_now.humidity IS '湿度，百分比';