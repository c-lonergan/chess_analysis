import logging
from pathlib import Path

from config import APP_NAME, LOG_DIR

def get_logger(name: str = None, log_file: str = None) -> logging.Logger:
    name = name or APP_NAME
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    
    if log_file:
        log_path = Path(f"{LOG_DIR}/{log_file}")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        fh = logging.FileHandler(log_path, mode='a', encoding='utf-8')
        fh.setLevel(logging.INFO)
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    
    return logger

if __name__ == "__main__":
    logger = get_logger(log_file='test_log')
    logger.info("test")