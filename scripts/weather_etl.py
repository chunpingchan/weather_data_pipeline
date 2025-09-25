#!/usr/bin/env python3
"""
和风天气数据ETL管道
功能：从和风天气API获取数据，清洗转换后存储到PostgreSQL
"""

import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
import json
from typing import Dict, Any, Optional

# 配置日志
def setup_logging():
    """配置日志格式和级别"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # 输出到控制台
            logging.FileHandler('/opt/airflow/logs/weather_etl.log')  # 输出到文件
        ]
    )
    return logging.getLogger(__name__)

class WeatherETL:
    """天气数据ETL处理器"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.api_key = self._get_env_variable('QWEATHER_API_KEY')
        self.api_host = self._get_env_variable('QWEATHER_API_HOST')
        self.locations = self._get_env_variable('QWEATHER_LOCATIONS', '').split(',')
        self.location_names = self._get_env_variable('QWEATHER_LOCATION_NAMES', '').split(',')
        self.db_engine = self._create_db_engine()
        
    def _get_env_variable(self, var_name: str, default: str = None) -> str:
        """获取环境变量，如果不存在则抛出异常"""
        value = os.environ.get(var_name, default)
        if value is None:
            raise ValueError(f"Environment variable {var_name} is required")
        return value
    
    def _create_db_engine(self):
        """创建数据库连接引擎"""
        db_user = self._get_env_variable('POSTGRES_USER')
        db_password = self._get_env_variable('POSTGRES_PASSWORD')
        db_name = self._get_env_variable('POSTGRES_DB')
        db_host = self._get_env_variable('POSTGRES_HOST', 'postgres')
        
        connection_str = f'postgresql://{db_user}:{db_password}@{db_host}:5432/{db_name}'
        return create_engine(connection_str, pool_size=5, max_overflow=10)
    
    def fetch_weather_data(self, location_id: str, location_name: str) -> Optional[Dict[str, Any]]:
        """从和风天气API获取数据"""
        url = f"https://{self.api_host}/v7/weather/now?location={location_id}&key={self.api_key}"
        
        try:
            self.logger.info(f"Fetching data for {location_name} ({location_id})")
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if data['code'] == '200':
                return self._transform_data(data, location_name, location_id)
            else:
                self.logger.error(f"API error for {location_name}: {data}")
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {location_name}: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error for {location_name}: {str(e)}")
            return None
    
    def _transform_data(self, raw_data: Dict, location_name: str, location_id: str) -> Dict[str, Any]:
        """转换和清洗数据"""
        now_data = raw_data['now']
        
        return {
            'city_name': location_name,
            'city_id': location_id,
            'update_time': datetime.fromisoformat(raw_data['updateTime'].replace('Z', '+00:00')),
            'temp': float(now_data['temp']),
            'feels_like': float(now_data['feelsLike']),
            'text': now_data['text'],
            'wind_scale': now_data['windScale'],
            'humidity': int(now_data['humidity']),
            'pressure': int(now_data.get('pressure', 0)) if now_data.get('pressure') else None,
            'vis': int(now_data.get('vis', 0)) if now_data.get('vis') else None,
            'cloud': now_data.get('cloud', ''),
            'insert_ts': datetime.now()
        }
    
    def validate_data(self, data: Dict[str, Any]) -> bool:
        """验证数据有效性"""
        required_fields = ['temp', 'humidity', 'text']
        for field in required_fields:
            if field not in data or data[field] is None:
                return False
        
        # 验证数值范围
        if not (-50 <= data['temp'] <= 50):
            return False
        if not (0 <= data['humidity'] <= 100):
            return False
            
        return True
    
    def load_to_database(self, data_list: list):
        """加载数据到数据库"""
        if not data_list:
            self.logger.warning("No data to load")
            return
        
        try:
            df = pd.DataFrame(data_list)
            
            # 数据验证
            valid_data = []
            for _, row in df.iterrows():
                if self.validate_data(row.to_dict()):
                    valid_data.append(row)
            
            if not valid_data:
                self.logger.error("No valid data to insert")
                return
                
            valid_df = pd.DataFrame(valid_data)
            
            # 使用事务确保数据一致性
            with self.db_engine.begin() as connection:
                valid_df.to_sql(
                    'weather_now', 
                    connection, 
                    if_exists='append', 
                    index=False,
                    method='multi'  # 批量插入提高性能
                )
            
            self.logger.info(f"Successfully inserted {len(valid_df)} records")
            
        except Exception as e:
            self.logger.error(f"Failed to load data: {str(e)}")
            raise
    
    def run(self):
        """运行完整的ETL流程"""
        self.logger.info("Starting Weather ETL Pipeline")
        
        all_data = []
        success_count = 0
        
        for loc_id, loc_name in zip(self.locations, self.location_names):
            weather_data = self.fetch_weather_data(loc_id, loc_name)
            if weather_data:
                all_data.append(weather_data)
                success_count += 1
                self.logger.debug(f"Successfully processed {loc_name}")
        
        self.logger.info(f"Fetched data for {success_count}/{len(self.locations)} cities")
        
        if all_data:
            self.load_to_database(all_data)
        else:
            self.logger.warning("No data was fetched from any city")
        
        self.logger.info("Weather ETL Pipeline completed")

def main():
    """主函数"""
    try:
        etl = WeatherETL()
        etl.run()
    except Exception as e:
        logging.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()