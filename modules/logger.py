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
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(AuditFormatter())
        logger.addHandler(handler)
        
    return logger

# Singleton instance
logger = setup_logger()
