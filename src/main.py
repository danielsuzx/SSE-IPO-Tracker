import os
import sys
import time
import random
from loguru import logger
from collector import DataCollector
from processor import DataProcessor
from storage import LocalStorage

def setup_logger():
    """配置 loguru 结构化落盘"""
    logger.remove()
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base_dir, '.logs')
    os.makedirs(log_dir, exist_ok=True)
    
    logger.add(sys.stderr, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>", level="INFO")
    logger.add(os.path.join(log_dir, "pipeline_sse_{time:YYYY-MM-DD}.log"), rotation="50 MB", retention="10 days", level="INFO")
    logger.add(os.path.join(log_dir, "error_alerts_sse_{time:YYYY-MM-DD}.log"), level="ERROR", backtrace=True, diagnose=True)

def run_sse_pipeline():
    logger.info("=== 启动 Phase 4: 上交所 IPO 调度集群 ===")
    
    try:
        # 1. 引擎初始化
        collector = DataCollector()
        processor = DataProcessor()
        storage = LocalStorage()
        raw_dir, processed_dir = storage.setup_directories()
        
        # 2. Stage 1: 抓起 IPO 排队列表
        raw_list = collector.fetch_index_list()
        shared_timestamp = storage.save_raw_snapshot(raw_dir, raw_list, prefix="sse_ipo_index")
        
        target_list = processor.validate_and_extract_index(raw_list)
        logger.info(f"[*] [Scheduler] 成功从 Stage 1 大盘截获 {len(target_list)} 个过会标的。")
        
        # 3. Stage 2: 级联遍历每个审计节点拉取详细时序 (全量抓取)
        flattened_dataset = []
        total_targets = len(target_list)
        
        for idx, target in enumerate(target_list):
            audit_id = target['stockAuditNum']
            company = target['stockAuditName']
            logger.info(f"[*] [Scheduler] ({idx+1}/{total_targets}) 尝试下钻抓取 [{company}] 的历史时间轴...")
            
            raw_detail = collector.fetch_timeline_detail(audit_id)
            
            # (可选) 每次详情也保留 Snapshot 证据
            # storage.save_raw_snapshot(os.path.join(raw_dir, "details"), raw_detail, prefix=f"sse_detail_{audit_id}")
            
            flat_record = processor.flatten_timeline(company_name=company, audit_id=audit_id, raw_detail_data=raw_detail)
            if flat_record:
                flattened_dataset.append(flat_record)
                logger.success(f"    └── 拍平清洗完毕: {company}")
                
            # 全球爬虫铁律：模拟人肉随机操作延时
            time.sleep(random.uniform(0.5, 1.5))
            
        # 4. 汇总拍平的集群库双轨落盘
        storage.save_processed_data(processed_dir, flattened_dataset, prefix="sse_ipo_flattened_board", timestamp=shared_timestamp)
        logger.info("=== Phase 4 上交所 Pipeline 稳健执行完毕 ===")
        
    except Exception as e:
        logger.exception(f"Pipeline 主线程捕捉到无法自愈的致命故障，系统停机！信息: {e}")

if __name__ == '__main__':
    setup_logger()
    run_sse_pipeline()
