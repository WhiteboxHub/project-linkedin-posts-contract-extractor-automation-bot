import logging
import json
import sys
from datetime import datetime

class AuditFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings with fields:
    timestamp, level, message, post_id, step_name, exception_type.
    """
    def format(self, record):
        log_record = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "step_name": getattr(record, "step_name", None),
            "post_id": getattr(record, "post_id", None)
        }

        if record.exc_info:
            log_record["exception_type"] = record.exc_info[0].__name__
            # Optional: Include full traceback in message or separate field if needed
            # log_record["traceback"] = self.formatException(record.exc_info)

        return json.dumps(log_record)

def setup_logger(name="LinkedInBot"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Check if handlers already exist to avoid duplicate logs
    if not logger.handlers:
        # Stream Handler (Console)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(AuditFormatter())
        logger.addHandler(handler)
        
        # File Handler (Disk)
        try:
            import os
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            
            filename = f"{log_dir}/linkedin_bot_{datetime.now().strftime('%Y-%m-%d')}.log"
            file_handler = logging.FileHandler(filename, encoding='utf-8')
            file_handler.setFormatter(AuditFormatter())
            logger.addHandler(file_handler)
        except Exception as e:
            # Fallback if file logging fails, don't crash the app
            print(f"Failed to setup file logging: {e}")
        
    return logger

# Singleton instance
logger = setup_logger()
