
from pkgutil import walk_packages
import inspect
import logging
log = logging.getLogger(__name__)

from nuevo.core.engine import create_engine
from nuevo.core.elements import Vertex, Edge, VertexProxy, EdgeProxy
from nuevo.core.indices import IndexProxy
from nuevo.ogm.model import Node, NodeProxy

class Session(object):
    """
    Session object that connects to the database.
    
    Keeps indices for retrieving models, by the model element_type field.
    A Post model class with element_type = "posts" can be accessed like:
    
    db = DBSession()
    db.posts.get(1)
    db.posts.index.get(key=value) 
    """
    
    @classmethod
    def bind(cls, engine_uri):
        cls.engine_uri = engine_uri
    
    def __init__(self):
        self.engine = create_engine(self.engine_uri)
        
        # Standard stuff
        self.indices  = IndexProxy(self.engine)
        self.vertices = VertexProxy(self.engine)
        self.edges    = EdgeProxy(self.engine)
        
        
        # Models
        model_indices = {}
        for name in Node._model_registry:
            model = Node._model_registry[name]
            index = self.indices.get_create(name, Node)
            model_indices[name] = index
        
        self.nodes    = NodeProxy(engine=self.engine, indices=model_indices)
    