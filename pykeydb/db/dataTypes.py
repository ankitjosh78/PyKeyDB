from enum import Enum
from typing import Any, Dict
from dataclasses import dataclass


class DataType(Enum):
    STRING = "string"
    LIST = "list"
    HASH = "hash"
    SET = "set"
    INT = "int"
    FLOAT = "float"


@dataclass
class TypedValue:
    value: Any
    data_type: DataType

    def __init__(self, value, data_type):
        self.value = value
        self.data_type = data_type

    def to_dict(self) -> Dict:
        return {"type": self.data_type.value, "value": self._serialize_value()}

    def _serialize_value(self):
        # Convert set to list (for storing in JSON in WAL)
        if self.data_type == DataType.SET:
            return list(self.value)
        # Rest, integers and lists can be stored as it is. Dicts are also stored as it is.
        return self.value

    @staticmethod
    def from_dict(data: Dict) -> 'TypedValue':
        # Deserialize from dict from JSON in WAL.
        data_type = DataType(data["type"])
        value = data["value"]

        if data_type == DataType.SET:
            value = set(value)
        elif data_type == DataType.LIST:
            value = list(value)
        elif data_type == DataType.HASH:
            value = dict(value)
        elif data_type == DataType.INT:
            value = int(value)
        elif data_type == DataType.FLOAT:
            value = float(value)
        elif data_type == DataType.STRING:
            value = str(value)
        return TypedValue(value=value, data_type=data_type)
