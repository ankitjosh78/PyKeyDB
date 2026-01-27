from dataclasses import dataclass
from typing import Any, Dict, Optional
import threading
from logging import getLogger
from pykeydb.db.writeAheadLog import WriteAheadLog
from pykeydb.db.keyValueDBInterface import KeyValueDBInterface

logger = getLogger(__name__)


@dataclass
class PyKeyDB(KeyValueDBInterface):
    _instance = None
    _is_initialized = False
    _lock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, write_ahead_log: WriteAheadLog):
        if type(self)._is_initialized:
            return

        with type(self)._lock:
            if type(self)._is_initialized:
                return
            self._db: Dict[str, Any] = {}
            self._db_lock = threading.RLock()
            self.wal = write_ahead_log
            for record in self.wal.replay():
                if record:
                    try:
                        if record["operation"] == "SET":
                            key = record["key"]
                            value = record["value"]
                            self._db[key] = value
                        elif record["operation"] == "DEL":
                            key = record["key"]
                            if key in self._db:
                                del self._db[key]
                    except Exception:
                        logger.warning("Failed to replay WAL entry")

            logger.info("WAL entries replayed and updated in PyKeyDB...")
            logger.info("PyKeyDB Store Initialized...")
            type(self)._is_initialized = True

    @classmethod
    def dispose(cls):
        if cls._instance:
            try:
                if getattr(cls._instance, "wal", None):
                    cls._instance.wal.dispose()
            finally:
                cls._instance = None
                cls._is_initialized = False

    def set(self, key, value):
        with self._db_lock:
            try:
                self.wal.log_set(key, value)
                self._db[key] = value
                return True
            except Exception:
                logger.error(
                    f"SET operation failed. There was an error trying to set key: {key} with value: {value}"
                )
                return False

    def get(self, key):
        with self._db_lock:
            entry = self._db.get(key, None)
            return entry

    def delete(self, key):
        with self._db_lock:
            if key in self._db:
                try:
                    self.wal.log_del(key)
                    del self._db[key]
                    return True
                except Exception:
                    logger.error(f"There was an error trying to delete key: {key}")
                    return False
            else:
                logger.warning(f"DEL operation failed. The key: {key} doesn't exist.")
                return False

    def type(self, key):
        with self._db_lock:
            entry = self._db.get(key, None)
            if entry:
                return str(type(entry).__name__)
            logger.warning(f"TYPE operation failed. The key: {key} doesn't exist.")
            return None


_pykey_db: Optional[PyKeyDB] = None


def get_pykey_db(write_ahead_log: Optional[WriteAheadLog] = None):
    global _pykey_db
    if _pykey_db is None:
        if write_ahead_log is None:
            from pykeydb.db.writeAheadLog import get_write_ahead_log

            write_ahead_log = get_write_ahead_log()
        _pykey_db = PyKeyDB(write_ahead_log)
    return _pykey_db
