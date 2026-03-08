import os
import json
import time
import pandas as pd
from datetime import datetime
from loguru import logger

class LocalStorage:
    """存储层：负责本地物理磁盘的双轨落盘与拓扑结构管理"""
    def __init__(self, base_dir=None):
        if base_dir is None:
            self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        else:
            self.base_dir = base_dir
            
    def setup_directories(self):
        """初始化并按日期分层构建本地存储目录"""
        today_str = datetime.now().strftime('%Y-%m-%d')
        raw_dir = os.path.join(self.base_dir, '.data_pipeline', 'raw', today_str)
        processed_dir = os.path.join(self.base_dir, '.data_pipeline', 'processed', today_str)
        
        os.makedirs(raw_dir, exist_ok=True)
        os.makedirs(processed_dir, exist_ok=True)
        
        return raw_dir, processed_dir

    def save_raw_snapshot(self, raw_dir, data, prefix="snapshot"):
        """以完整 JSON 格式留存抓取快照，供日后审计与回放"""
        timestamp = int(time.time())
        raw_file = os.path.join(raw_dir, f"{prefix}_raw_{timestamp}.json")
        try:
            with open(raw_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"[Storage] 原始快照已落盘: {raw_file}")
            return timestamp
        except IOError as e:
            logger.critical(f"[Storage] 原始快照落盘失败! {e}")
            raise e

    def save_processed_data(self, processed_dir, extracted_data, prefix="snapshot", timestamp=None):
        """将清洗后的结构化字典存储为 CSV 格式"""
        if not extracted_data:
            logger.warning("[Storage] 无清洗数据需要落盘。")
            return None
            
        if timestamp is None:
            timestamp = int(time.time())
            
        try:
            df = pd.DataFrame(extracted_data)
            processed_file = os.path.join(processed_dir, f"{prefix}_clean_{timestamp}.csv")
            df.to_csv(processed_file, index=False, encoding='utf-8-sig')
            logger.success(f"[Storage] 结构化业务数据成功写入: {processed_file}")
            return processed_file
        except Exception as e:
            logger.critical(f"[Storage] 数据落盘写入 CSV 失败! {e}")
            raise e
