
from nose.tools import with_setup, make_decorator

from . import with_engine

@with_engine
def test_dummy(engine):
    pass

