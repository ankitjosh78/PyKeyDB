import threading
import os
from typing import Dict, List, Optional, Any
import json
from logging import getLogger
from pykeydb.db.dataTypes import DataType

logger = getLogger(__name__)


class WriteAheadLog:
    _instances: Dict[str, 'WriteAheadLog'] = {}
    _lock = threading.Lock()

    def __new__(cls, path="wal.log", use_fsync=False):
        with cls._lock:
            if path not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[path] = instance
            return cls._instances[path]

    def __init__(self, path="wal.log", use_fsync=False):
        if self._initialized:
            return

        with type(self)._lock:
            if self._initialized:
                return
            self.path = path
            self.use_fsync = use_fsync
            self.file_writer = open(self.path, "a+", buffering=1)
            logger.info(f"WriteAheadLog writer initialized for path: {path}")
            self.wal_lock = threading.RLock()
            self._initialized = True

    @classmethod
    def dispose(cls, path: Optional[str] = None):
        """Dispose WAL instance(s). If path is None, dispose all instances."""
        with cls._lock:
            if path is not None:
                # Dispose specific path
                if path in cls._instances:
                    instance = cls._instances[path]
                    try:
                        if getattr(instance, "file_writer", None):
                            instance.file_writer.close()
                            logger.info(f"WriteAheadLog closed for path: {path}")
                    except Exception as e:
                        logger.exception(f"Failed to close WriteAheadLog for {path}: {e}")
                        raise
                    finally:
                        del cls._instances[path]
            else:
                # Dispose all instances
                for p, instance in list(cls._instances.items()):
                    try:
                        if getattr(instance, "file_writer", None):
                            instance.file_writer.close()
                            logger.info(f"WriteAheadLog closed for path: {p}")
                    except Exception as e:
                        logger.exception(f"Failed to close WriteAheadLog for {p}: {e}")
                cls._instances.clear()

    def log_operation(
        self, operation: str, key: str, value_dict: Optional[Dict] = None, **kwargs
    ):
        """Generic operation logger with type info"""
        with self.wal_lock:
            entry: Dict[str, Any] = {
                "operation": operation,
                "key": key,
            }
            if value_dict is not None:
                entry["value"] = value_dict
            if kwargs:
                entry.update(kwargs)
            self.file_writer.write(json.dumps(entry) + "\n")
            if self.use_fsync:
                self.file_writer.flush()
                os.fsync(self.file_writer.fileno())

    def log_set(self, key, value):
        """Legacy method - kept for backward compatibility"""
        with self.wal_lock:
            entry = {"operation": "SET", "key": key, "value": value}
            self.file_writer.write(json.dumps(entry) + "\n")
            if self.use_fsync:
                self.file_writer.flush()
                os.fsync(self.file_writer.fileno())

    def log_del(self, key):
        """Legacy method - kept for backward compatibility"""
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


_write_ahead_logs: Dict[str, WriteAheadLog] = {}
_wal_factory_lock = threading.Lock()


def get_write_ahead_log(path="wal.log", use_fsync=False) -> WriteAheadLog:
    """Get or create WAL instance for the given path (singleton per path)"""
    with _wal_factory_lock:
        if path not in _write_ahead_logs:
            _write_ahead_logs[path] = WriteAheadLog(path, use_fsync)
        return _write_ahead_logs[path]


def dispose_write_ahead_log(path: str):
    """Dispose specific WAL instance and remove from factory cache"""
    with _wal_factory_lock:
        if path in _write_ahead_logs:
            del _write_ahead_logs[path]
    WriteAheadLog.dispose(path)
