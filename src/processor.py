from loguru import logger
from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Any

# ==========================================
# Schema Definition (SSE IPO Dictionary)
# ==========================================
class SSEProjectIndexItem(BaseModel):
    """上交所 Stage 1 列表节点强校验模型"""
    stockAuditName: str
    stockAuditNum: str

    @field_validator('stockAuditName', 'stockAuditNum')
    def prevent_nulls(cls, v, info):
        if v is None:
            raise ValueError(f"Field '{info.field_name}' in Stage 1 cannot be None.")
        return v

class SSETimeNode(BaseModel):
    """上交所 Stage 2 时序节点强校验模型"""
    publishDate: str
    auditStatus: int

    @field_validator('publishDate', 'auditStatus')
    def prevent_nulls(cls, v, info):
        if v is None:
            raise ValueError(f"Field '{info.field_name}' in Stage 2 cannot be None.")
        return v

class FlatIPOProject(BaseModel):
    """最终拍平输出的业务数据宽表"""
    Company_Name: str
    Audit_ID: str
    Date_Received: str = ""        # Status 1: 已受理
    Date_Inquired: str = ""        # Status 2: 已问询
    Date_Committee_Meeting: str = "" # Status 3: 上市委会议
    Date_Submit_Register: str = ""   # Status 4: 提交注册
    Date_Register_Result: str = ""   # Status 5: 注册结果
    Date_Suspend: str = ""           # Status 6: 中止
    Date_Halt: str = ""              # Status 7: 终止


class DataProcessor:
    """数据清洗层：拍平字典重组宽表模块"""
    
    def validate_and_extract_index(self, raw_list_data: dict) -> List[dict]:
        """校验目录页并踢出可被抓取的列队"""
        logger.info("[Processor] 执行 Stage 1 目录提取...")
        if 'pageHelp' not in raw_list_data or 'data' not in raw_list_data['pageHelp']:
             raise KeyError("[Fatal] Stage 1 接口格式异形，缺失 pageHelp.data")
             
        pool = raw_list_data['pageHelp']['data']
        extracted_index = []
        for idx, item in enumerate(pool):
            try:
                validated = SSEProjectIndexItem(**item)
                extracted_index.append({
                    "stockAuditName": validated.stockAuditName,
                    "stockAuditNum": validated.stockAuditNum
                })
            except Exception as e:
                logger.error(f"[Processor] Stage 1 节点 {idx} 结构异形跳过: {e}")
                
        return extracted_index

    def flatten_timeline(self, company_name: str, audit_id: str, raw_detail_data: dict) -> dict:
        """拍平时序列表"""
        if 'pageHelp' not in raw_detail_data or 'data' not in raw_detail_data['pageHelp']:
            raise KeyError(f"[Fatal] Stage 2 接口格式异形，缺失 pageHelp.data (Target: {audit_id})")
            
        nodes = raw_detail_data['pageHelp']['data']
        
        flat_record = FlatIPOProject(Company_Name=company_name, Audit_ID=audit_id)
        
        for item in nodes:
            try:
                val = SSETimeNode(**item)
            except Exception as e:
                logger.error(f"[Processor] Stage 2 时效异常 (Target: {audit_id}): {e}")
                continue
                
            # Dictionary Map
            if val.auditStatus == 1:
                flat_record.Date_Received = val.publishDate
            elif val.auditStatus == 2:
                # 若存在多次问询取最新，因为做了 order desc 这默认是最新的一条问询（取决于取值顺序，如果从后往前需要注意覆盖）
                if not flat_record.Date_Inquired:
                    flat_record.Date_Inquired = val.publishDate
            elif val.auditStatus == 3:
                if not flat_record.Date_Committee_Meeting:
                    flat_record.Date_Committee_Meeting = val.publishDate
            elif val.auditStatus == 4:
                flat_record.Date_Submit_Register = val.publishDate
            elif val.auditStatus == 5:
                flat_record.Date_Register_Result = val.publishDate
            elif val.auditStatus == 6:
                flat_record.Date_Suspend = val.publishDate
            elif val.auditStatus == 7:
                flat_record.Date_Halt = val.publishDate
                
        return flat_record.model_dump(mode='json')
