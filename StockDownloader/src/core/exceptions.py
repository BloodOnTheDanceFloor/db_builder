from concurrent.futures import ThreadPoolExecutor, TimeoutError
from functools import wraps


def timeout_decorator(seconds):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except TimeoutError:
                    raise DataFetchError(f"Operation timed out after {seconds} seconds")

        return wrapper

    return decorator


class DataFetchError(Exception):
    """当数据获取失败时抛出的异常"""
    pass


class DataSaveError(Exception):
    """数据保存异常"""
    pass


class DatabaseError(Exception):
    """数据库操作异常"""
    pass


class ConfigError(Exception):
    """配置错误异常"""
    pass
