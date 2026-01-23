from abc import ABC, abstractmethod
from typing import Optional, Any


class KeyValueDBInterface(ABC):
    @abstractmethod
    def set(self, key: str, value: Any) -> bool:
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        pass

    @abstractmethod
    def type(self, key: str) -> Optional[str]:
        pass
