
from nuevo.core.elements import VertexProxy, EdgeProxy
from nuevo.core.indices import IndexProxy

class Graph(object):
    
    def __init__(self, engine):
        self._engine = engine
        self._vertices = VertexProxy(engine)
        self._edges = EdgeProxy(engine)
        self._indices = IndexProxy(engine)
    
    @property
    def vertices(self):
        return self._vertices
    
    @property
    def edges(self):
        return self._edges
    
    @property
    def indices(self):
        return self._indices