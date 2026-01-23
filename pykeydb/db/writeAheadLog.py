import threading
import os
from typing import Dict, List, Optional
import json
from logging import getLogger

logger = getLogger(__name__)


class WriteAheadLog:
    _instance = None
    _is_initialized = False
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, path="wal.log", use_fsync=False):
        if type(self)._is_initialized:
            return

        with type(self)._lock:
            if type(self)._is_initialized:
                return
            self.path = path
            self.use_fsync = use_fsync
            self.file_writer = open(self.path, "a+", buffering=1)
            logger.info("WriteAheadLog writer initialized...")
            self.wal_lock = threading.RLock()
            type(self)._is_initialized = True

    @classmethod
    def dispose(cls):
        if cls._instance:
            try:
                if getattr(cls._instance, "file_writer", None):
                    cls._instance.file_writer.close()
                    logger.info("WriteAheadLog closed...")
            except Exception as e:
                logger.exception(f"Failed to close WriteAheadLog: {e}")
                raise
            finally:
                cls._instance = None
                cls._is_initialized = False

    def log_set(self, key, value):
        with self.wal_lock:
            entry = {"operation": "SET", "key": key, "value": value}
            self.file_writer.write(json.dumps(entry) + "\n")
            if self.use_fsync:
                self.file_writer.flush()
                os.fsync(self.file_writer.fileno())

    def log_del(self, key):
        with self.wal_lock:
            entry = {"operation": "DEL", "key": key}
            self.file_writer.write(json.dumps(entry) + "\n")
            if self.use_fsync:
                self.file_writer.flush()
                os.fsync(self.file_writer.fileno())

    def replay(self) -> List[Dict]:
        operations = []
        if not os.path.exists(self.path):
            return operations
        with self.wal_lock:
            with open(self.path, "r") as wal_file:
                for line in wal_file:
                    try:
                        operations.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning("Skipping corrupt WAL entry.")
        return operations


_write_ahead_log: Optional[WriteAheadLog] = None


def get_write_ahead_log() -> WriteAheadLog:
    global _write_ahead_log
    if _write_ahead_log is None:
        _write_ahead_log = WriteAheadLog()
    return _write_ahead_log
