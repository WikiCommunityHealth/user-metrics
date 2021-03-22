import time
from datetime import datetime


def log(message: str) -> None:
    print(message, f'{datetime.now():%Y-%m-%dT%H:%M:%SZ}')


def logSeconds(message: str) -> None:
    print(message, time.time())


def retrieve_time():
    return time.time()
