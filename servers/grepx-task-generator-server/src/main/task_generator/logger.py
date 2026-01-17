import logging
from datetime import datetime
from pathlib import Path


def setup_logger(name: str = "task-generator"):
    """
    Setup logger with file and console handlers
    Log file format: {name}_{date}.log
    """
    # Get log directory from root of pricing app
    root_dir = Path(__file__).parent.parent.parent.parent.parent.parent
    log_dir = root_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with date
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"{name}_{date_str}.log"
    
    # Configure logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logger initialized. Logging to: {log_file}")
    
    return logger
