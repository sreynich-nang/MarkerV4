import time
from contextlib import contextmanager

@contextmanager
def timer(name: str = "task"):
    start = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start
        print(f"{name} took {elapsed:.2f}s")
