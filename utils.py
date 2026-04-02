import os
import sys
import logging
import json
import yaml
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path

CONFIG_FILE = "config.yaml"
DEFAULT_CONFIG = {
    "log_level": "INFO",
    "log_file": "scraper.log",
    "log_max_bytes": 10485760,
    "log_backup_count": 5,
    "delay_min": 1.0,
    "delay_max": 2.5,
    "max_retries": 3,
    "retry_backoff": 1.5,
    "timeout": 15,
    "output_dir": "output",
    "date_format": "%Y-%m-%d_%H-%M-%S",
    "email_verification": {
        "use_cache": True,
        "cache_ttl_days": 7,
        "deep_verify": True
    }
}

class Config:
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_config()
        return cls._instance
    
    def _load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    self._config = yaml.safe_load(f)
                print(f"[CONFIG] Loaded settings from {CONFIG_FILE}")
            except Exception as e:
                print(f"[CONFIG] Failed to load config: {e}. Using defaults.")
                self._config = DEFAULT_CONFIG.copy()
        else:
            self._config = DEFAULT_CONFIG.copy()
            print(f"[CONFIG] Using default settings (no config.yaml found)")
    
    def get(self, key, default=None):
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k, default)
            else:
                return default
        return value if value is not None else default
    
    def get_all(self):
        return self._config

def setup_logging(name=None):
    config = Config().get_all()
    log_level = getattr(logging, config.get('log_level', 'INFO').upper())
    log_file = config.get('log_file', 'scraper.log')
    max_bytes = config.get('log_max_bytes', 10485760)
    backup_count = config.get('log_backup_count', 5)
    
    log_dir = Path(log_file).parent
    if log_dir and not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)
    
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)-8s [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    if root_logger.handlers:
        root_logger.handlers.clear()
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_bytes, backupCount=backup_count
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    logger = logging.getLogger(name or __name__)
    logger.info(f"Logging initialized (level: {logging.getLevelName(log_level)})")
    return logger

class ScraperLogger:
    def __init__(self, name=None):
        self.logger = setup_logging(name)
    
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)
    
    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)
    
    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)
    
    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)
    
    def exception(self, msg, *args, **kwargs):
        self.logger.exception(msg, *args, **kwargs)

def get_timestamp():
    config = Config().get_all()
    fmt = config.get('date_format', '%Y-%m-%d_%H-%M-%S')
    return datetime.now().strftime(fmt)

def ensure_output_dir():
    config = Config().get_all()
    output_dir = config.get('output_dir', 'output')
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    return output_dir

if __name__ == "__main__":
    logger = ScraperLogger("test")
    logger.info("Test info message")
    logger.warning("Test warning message")
    logger.error("Test error message")