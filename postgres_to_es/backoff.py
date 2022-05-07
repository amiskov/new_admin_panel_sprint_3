import logging
from functools import wraps
from time import sleep
from typing import Callable


def backoff(start_sleep_time: float = 0.1,
            factor: int = 2,
            border_sleep_time: int = 10) -> Callable:
    """
    Функция для повторного выполнения функции через некоторое время, если
    возникла ошибка. Использует наивный экспоненциальный рост времени повтора
    (factor) до граничного времени ожидания (border_sleep_time).

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func: Callable) -> Callable:
        @wraps(func)
        def inner(*args, **kwargs) -> Callable:
            sleep_time = start_sleep_time
            n = 1

            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as error:
                    logging.warning(f'{error}. Slept for {sleep_time}.')

                    if sleep_time < border_sleep_time:
                        sleep_time = min(start_sleep_time * (factor ** n),
                                         border_sleep_time)
                    else:
                        sleep_time = border_sleep_time

                    sleep(sleep_time)
                    n += 1
                    continue
                finally:
                    if n >= 500:
                        raise Exception(f'Too many attempts ({n})!')

        return inner

    return func_wrapper
