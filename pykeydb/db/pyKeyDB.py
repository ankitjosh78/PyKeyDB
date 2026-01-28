from typing import Any, Dict, Optional
import threading
from logging import getLogger
from pykeydb.db.writeAheadLog import WriteAheadLog
from pykeydb.db.keyValueDBInterface import KeyValueDBInterface
from pykeydb.db.dataTypes import TypedValue, DataType

logger = getLogger(__name__)


class PyKeyDB(KeyValueDBInterface):
    _instances: Dict[str, 'PyKeyDB'] = {}
    _lock = threading.RLock()

    def __new__(cls, write_ahead_log: WriteAheadLog):
        wal_path = write_ahead_log.path
        with cls._lock:
            if wal_path not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[wal_path] = instance
            return cls._instances[wal_path]

    def __init__(self, write_ahead_log: WriteAheadLog):
        if self._initialized:
            return

        with type(self)._lock:
            if self._initialized:
                return
            self._db: Dict[str, TypedValue] = {}
            self._db_lock = threading.RLock()
            self.wal = write_ahead_log
            for record in self.wal.replay():
                if record:
                    try:
                        op = record["operation"]
                        key = record["key"]

                        if op == "SET":
                            value = record["value"]
                            # Check if value is a dict with type info (new format)
                            if isinstance(value, dict) and "type" in value:
                                self._db[key] = TypedValue.from_dict(value)
                            else:
                                # Legacy format - treat as string
                                self._db[key] = TypedValue(value, DataType.STRING)

                        elif op == "DEL":
                            if key in self._db:
                                del self._db[key]

                        elif op in ["LPUSH", "RPUSH", "LPOP", "RPOP"]:
                            # List operations store full state
                            value = record["value"]
                            if isinstance(value, dict) and "type" in value:
                                self._db[key] = TypedValue.from_dict(value)

                    except Exception as e:
                        logger.warning(f"Failed to replay WAL entry: {e}")

            logger.info("WAL entries replayed and updated in PyKeyDB...")
            logger.info("PyKeyDB Store Initialized...")
            self._initialized = True

    @classmethod
    def dispose(cls, wal_path: Optional[str] = None):
        """Dispose PyKeyDB instance(s). If wal_path is None, dispose all instances."""
        with cls._lock:
            if wal_path is not None:
                # Dispose specific path
                if wal_path in cls._instances:
                    instance = cls._instances[wal_path]
                    try:
                        if getattr(instance, "wal", None):
                            instance.wal.dispose(wal_path)
                    finally:
                        del cls._instances[wal_path]
            else:
                # Dispose all instances
                for path, instance in list(cls._instances.items()):
                    try:
                        if getattr(instance, "wal", None):
                            instance.wal.dispose(path)
                    except Exception:
                        pass
                cls._instances.clear()

    def set(self, key, value):
        with self._db_lock:
            try:
                typed_val = TypedValue(value, DataType.STRING)
                self.wal.log_operation("SET", key, typed_val.to_dict())
                self._db[key] = typed_val
                return True
            except Exception as e:
                logger.error(f"SET operation failed. Error: {e}")
                return False

    def get(self, key):
        with self._db_lock:
            typed_val = self._db.get(key)
            if typed_val is None:
                return None
            elif typed_val.data_type != DataType.STRING:
                return "NULL"

            return typed_val.value

    def delete(self, key):
        with self._db_lock:
            if key in self._db:
                try:
                    self.wal.log_operation("DEL", key)
                    del self._db[key]
                    return True
                except Exception as e:
                    logger.error(f"Delete failed for key {key}: {e}")
                    return False
            else:
                return False

    def type(self, key):
        with self._db_lock:
            typed_val = self._db.get(key, None)
            if typed_val:
                return typed_val.data_type.value
            return None

    def lpush(self, key: str, *values: str):
        with self._db_lock:
            typed_val = self._db.get(key)

            if typed_val is None:
                typed_val = TypedValue(list(values), DataType.LIST)
            elif typed_val.data_type != DataType.LIST:
                raise TypeError(
                    f"ERR: WRONGTYPE -> key is {typed_val.data_type.value}, not list"
                )
            else:
                typed_val.value = list(values) + typed_val.value

            self.wal.log_operation("LPUSH", key, typed_val.to_dict())
            self._db[key] = typed_val
            return len(typed_val.value)

    def rpush(self, key: str, *values):
        with self._db_lock:
            typed_val = self._db.get(key)

            if typed_val is None:
                typed_val = TypedValue(list(values), data_type=DataType.LIST)
            elif typed_val.data_type != DataType.LIST:
                raise TypeError(
                    f"ERR: WRONGTYPE -> key is {typed_val.data_type.value}, not list"
                )
            else:
                typed_val.value = typed_val.value + list(values)

            self.wal.log_operation(
                operation="RPUSH", key=key, value_dict=typed_val.to_dict()
            )
            self._db[key] = typed_val
            return len(typed_val.value)

    def lrange(self, key, start, stop):
        with self._db_lock:
            typed_val = self._db.get(key)

            # If key doesn't exist, return empty list []
            if typed_val is None:
                return []
            # If value data type is not list
            elif typed_val.data_type != DataType.LIST:
                raise TypeError(
                    f"ERR: WRONGTYPE -> key is {typed_val.data_type.value}, not list"
                )
            # If -1, means give me the entire list from start to end
            if stop == -1:
                return typed_val.value[start:]
            # Else, normal list slicing
            return typed_val.value[start : stop + 1]

    def lpop(self, key):
        with self._db_lock:
            typed_val = self._db.get(key)

            # If key doesn't exist
            if typed_val is None:
                return None
            # If value data type is not list
            elif typed_val.data_type != DataType.LIST:
                raise TypeError(
                    f"ERR: WRONGTYPE -> key is {typed_val.data_type.value}, not list"
                )
            # If list is empty, then can't pop. So, return None
            if not typed_val.value:
                return None

            # List operations take place in reference in Python, so no need to do _db[key] = typed_val.value again
            element = typed_val.value.pop(0)
            self.wal.log_operation("LPOP", key, value_dict=typed_val.to_dict())

            # If we clear entire list, we can remove the key from db
            if not typed_val.value:
                del self._db[key]

            return element

    def rpop(self, key):
        with self._db_lock:
            typed_val = self._db.get(key)

            # If key doesn't exist
            if typed_val is None:
                return None
            # If value data type is not list
            elif typed_val.data_type != DataType.LIST:
                raise TypeError(
                    f"ERR: WRONGTYPE -> key is {typed_val.data_type.value}, not list"
                )
            # If list is empty, then can't pop. So, return None
            if not typed_val.value:
                return None

            # List operations take place in reference in Python, so no need to do _db[key] = typed_val.value again
            element = typed_val.value.pop()
            self.wal.log_operation("RPOP", key, value_dict=typed_val.to_dict())

            # If we clear entire list, we can remove the key from db
            if not typed_val.value:
                del self._db[key]

            return element

    def llen(self, key):
        with self._db_lock:
            typed_val = self._db.get(key)

            # If key doesn't exist, just return 0 elements are present
            if typed_val is None:
                return 0
            # If value data type is not list
            elif typed_val.data_type != DataType.LIST:
                raise TypeError(
                    f"ERR: WRONGTYPE -> key is {typed_val.data_type.value}, not list"
                )
            # Return length of the list
            return len(typed_val.value)


_pykey_dbs: Dict[str, PyKeyDB] = {}
_db_factory_lock = threading.RLock()


def get_pykey_db(write_ahead_log: Optional[WriteAheadLog] = None, wal_path: str = "wal.log") -> PyKeyDB:
    """Get or create PyKeyDB instance for the given WAL (singleton per WAL path)"""
    with _db_factory_lock:
        if write_ahead_log is None:
            from pykeydb.db.writeAheadLog import get_write_ahead_log
            write_ahead_log = get_write_ahead_log(wal_path)
        
        path = write_ahead_log.path
        if path not in _pykey_dbs:
            _pykey_dbs[path] = PyKeyDB(write_ahead_log)
        return _pykey_dbs[path]


def dispose_pykey_db(wal_path: str):
    """Dispose specific PyKeyDB instance and remove from factory cache"""
    with _db_factory_lock:
        if wal_path in _pykey_dbs:
            del _pykey_dbs[wal_path]
    PyKeyDB.dispose(wal_path)
