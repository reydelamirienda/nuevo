
from functools import wraps
from nuevo.core.engine import create_engine

import os

def with_engine(test):
    @wraps(test)
    def wrapper(*args, **kwargs):
        try:
            url = os.environ['NUEVO_TEST_ENGINE']
        except KeyError:
            raise Exception("Please define NUEVO_TEST_ENGINE with the URL string of the engine used in the test")
        engine = create_engine(url)
        engine._clear_database_for_testing()
        return test(*args, engine=engine, **kwargs)
    return wrapper
