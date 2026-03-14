import os 
import datetime
from src.utils.logger import get_logger

logger = get_logger(__name__)



class AlertSystem:
    def __init__(self, audit_log_path='logs/pipeline_audit.log'):
        self.audit_log_path = audit_log_path
        os.makedirs(os.path.dirname(audit_log_path), exist_ok=True)
    
    def notify_critical_failure(self, endpoint, error_msg, serverity='CRITICAL'):
        timestamp = datetime.datetime.now().isoformat()
        alert_msg = f"[{timestamp}] [{serverity}] Endpoint '{endpoint}': Message {error_msg}\n"
        with open(self.audit_log_path, 'a') as f:
            f.write(alert_msg)
        logger.error(alert_msg.strip())
        