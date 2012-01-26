
from nuevo.core.indices import Index

class Neo4jIndex(Index):
    
    def __init__(self, engine, content, element_type):
        self._engine = engine
        self._content = content
        self.element_type = element_type
    
    @property
    def uri(self):
        return self._content.uri
    
    