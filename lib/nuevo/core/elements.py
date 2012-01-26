"""
Vertex and Edge container classes and proxies.
"""

class Element(object):
    """This is an abstract base class for Vertex and Edge"""
    
    def __init__(self, engine, content, **kwargs):
        self._engine = engine
        self._content = content
        super(Element, self).__init__(**kwargs)
    
    @property
    def id(self):
        return self._content.id
    
    def __setitem__(self, key, value):
        """
        Create and/or assign a property of an Element that will be persisted.
        """
        self._content.data[key] = value
    
    def __getitem__(self, key):
        """
        Return a property of an Element.
        """
        return self._content.data[key]

    def __iter__(self):
        return self._content.data.__iter__()
    
    def __len__(self):
        return len(self._content.data)

    def __contains__(self, item):
        return item in self._content.data
    
    def __eq__(self, other):
        return isinstance(other, Element) \
            and self.id == other.id \
            and self._engine == other._engine \
            and self._content == other._content
    
    def __repr__(self):
        return u"<%s: %s>" % (self.__class__.__name__, self._content.uri)
    
class Vertex(Element):
    """A container for Vertex elements returned by the DB."""     
        
    def outE(self, *labels):
        """Return the outgoing edges of the vertex."""
        return self._engine.out_edges(self, labels)

    def inE(self, *labels):
        """Return the incoming edges of the vertex."""
        return self._engine.in_edges(self, labels)

    def bothE(self, *labels):
        """Return all incoming and outgoing edges of the vertex."""
        return self._engine.both_edges(self, labels)
    
    def outV(self, *labels):
        """Return the out-adjacent vertices to the vertex."""
        return self._engine.out_vertices(self, labels)

    def inV(self, *labels):
        """Return the in-adjacent vertices of the vertex."""
        return self._engine.in_vertices(self, labels)

    def bothV(self, *labels):
        """Return all incoming- and outgoing-adjacent vertices of vertex."""
        return self._engine.both_vertices(self, labels)

class Edge(Element):
    """A container for Edge elements returned by the resource."""

    @property
    def outV(self):
        """Returns the outgoing Vertex of the edge."""
        content = self._engine.out_vertex(self)
        return Vertex(self._engine, content)
    
    @property
    def inV(self):
        """Returns the incoming Vertex of the edge."""
        content = self._engine.in_vertex(self)
        return Vertex(self._engine, content)

class VertexProxy(object):
    """A proxy for interacting with vertices."""

    def __init__(self, engine, **kwargs):
        self._engine = engine

    def create(self, **kwargs):
        """Adds a vertex to the database and returns it."""
        content = self._engine.create_vertex(kwargs)
        return Vertex(self._engine, content)

    def get(self, id):
        """Retrieves a vertex from the DB and returns it."""
        content = self._engine.get_vertex(id)
        return Vertex(self._engine, content)

    def update(self, _vertex, **kwargs):
        """Updates a vertex in the graph DB and returns it.""" 
        return self._engine.update_vertex(_vertex, kwargs)
    
    def delete(self, _vertex):
        """Deletes a vertex from the graph DB."""
        return self._engine.delete_vertex(_vertex)

class EdgeProxy(object):
    """A proxy for interacting with edges."""

    def __init__(self, engine):
        self._engine = engine

    def create(self, _outV, _label, _inV, **kwargs):
        """Adds an edge to the database and returns it.""" 
        if not _label or _inV is None or _outV is None:
            raise ValueError("Invalid argument value")
        if not isinstance(_label, str):
            raise TypeError("_label must be a string")
        
        content = self._engine.create_edge(_outV, _label, _inV, kwargs)
        return Edge(self._engine, content)

    def get(self, id):
        """Retrieves an edge from the DB and returns it."""
        content = self._engine.get_edge(id)
        return Edge(self._engine, content)
        
    def update(self, _edge, **kwargs):
        """Updates an edge in the graph DB and returns it.""" 
        return self._engine.update_edge(_edge, kwargs)
    
    def delete(self, _edge):
        """Deletes a vertex from a graph DB and returns the response."""
        return self._engine.delete_edge(_edge)
