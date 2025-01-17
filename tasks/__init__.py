from abc import ABC
from typing import Callable

class task(ABC):
    def __init__(self, name: str) -> None:
        self._name = name
        self._next = []
        self._func = None
        
    def set(self, input_func: Callable):
        self._func = input_func
        
    def next(self, name: str|list[str]) -> None:
        self._next.append(name)
        
    def run(self):
        ...