import abc
import json
from pathlib import Path
from typing import Any, Optional

from redis import Redis


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    """
    Example:

    storage = JsonFileStorage('resources/storage.json')
    s = State(storage)
    s.set_state('last_modified', '2020-01-01 00:00:00')
    s.get_state('last_modified')
    """

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        Path(self.file_path).touch()

    def save_state(self, state):
        prev_state = self.retrieve_state()
        new_state = {**prev_state, **state}
        with open(self.file_path, 'w') as file:
            file.write(json.dumps(new_state))

    def retrieve_state(self):
        with open(self.file_path, 'r') as f:
            content = f.read()

            if not content:
                return {}

            return json.loads(content)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно
    не перечитывать данные с начала. Здесь представлена реализация
    с сохранением состояния в файл. В целом ничего не мешает поменять это
    поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state({key: value})

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state = self.storage.retrieve_state()
        return state.get(key)


class RedisStorage(BaseStorage):
    """
    Вместо файловой системы используйте key-value-хранилище наподобие Redis.
    Для знакомства с работой в Redis используйте статью из учебника Python.
    В вашем решении должен быть класс RedisStorage, который вместо имени файла
    принимает на вход объект соединения с Redis. Всё остальное остаётся
    таким же: нужно читать и записывать состояние. Для тестирования
    используйте файл с тестами Python💾.
    """

    def __init__(self, redis_adapter: Redis):
        self.redis_adapter = redis_adapter
