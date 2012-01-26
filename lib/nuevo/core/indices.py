"""
Index and IndexProxy classes.
"""

class Index(object):
    """Base Index class."""
    
    def __init__(self, engine, content, name, element_type):
        self._engine = engine
        self._content = content
        self.element_type = element_type
        self.name = name

    def __eq__(self, other):
        return isinstance(other, Index) \
            and self.element_type == other.element_type \
            and self._engine == other._engine \
            and self._content == other._content

    def put(self, element, key, value):
        """
        Append an element into the index at key/value.
        """
        return self._engine.index_put(self, element, key, value)

    def put_unique(self, element, key, value):
        """
        Put an element into the index at key/value and overwrite it if an 
        element already exists at that key and value.
        """
        return self._engine.index_put_unique(self, element, key, value)

    def remove(self, element, key=None, value=None):
        """Remove the element from the index completely, or from all entries with key,
        or from the entry for key/value."""
        self._engine.index_remove(self, element, key, value)

    def lookup(self, key, value):
        """Return all the elements that match the given key and value. Engine independent"""
        return self._engine.index_lookup(self, key, value)

    def query(self, query):
        """Returns all the elements that match the given auery, engine dependent!"""
        return self._engine.index_query(self, query)


class IndexProxy(object):
    """Index proxy to create and retrieve indices."""

    def __init__(self, engine):
        self._engine = engine
    
    def create(self, name, element_type, **config):
        """Creates an an index and returns it. Raises if the index exists."""
        resp = self._engine.index_create(name, element_type, config)
        return Index(self._engine, resp, name, element_type)

    def get(self, name, element_type):
        """Returns the Index object with the specified name
        or raise exception if not found and create is False."""
        resp = self._engine.index_get(name, element_type)
        return Index(self._engine, resp, name, element_type)
    
    def get_create(self, name, element_type, **config):
        resp = self._engine.index_get_create(name, element_type, config)
        return Index(self._engine, resp, name, element_type)

    def delete(self, index, element_type=None):
        """Deletes/drops an index (Index object, or by name&type)"""
        self._engine.index_delete(index, element_type)
        