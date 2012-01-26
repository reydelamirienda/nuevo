
class NotReadyException(Exception):
    pass

class Future(object):
    
    def __init__(self, cid=None, response=None):
        assert cid is not None or response is not None
        self._cid = cid
        self._response = response
    
    def __response_eq__(self, resp1, resp2):
        return resp1 == resp2
    
    def __eq__(self, other):
        return type(self) == type(other) \
            and ( 
                 ( not other.ready and not self.ready and self._cid == other._cid ) 
                 or 
                 ( other.ready and self.ready and self.__response_eq__(self._response, other._response) )
                )
    
    def __materialize__(self, response):
        assert response is not None
        self._response = response

    @property
    def ready(self):
        return self._response is not None

    def _raise_not_ready(self):
        if not self.ready:
            raise NotReadyException("This is still a future object")
    
class Neo4jContent(Future):
    
    @property
    def uri(self):
        try:
            return self._uri
        except NotReadyException:
            return "{%d}" % self._cid    
    
    def __setitem__(self, key, value):
        """
        Create and/or assign a property of an Element that will be persisted.
        """
        self._raise_not_ready()
        self._response[key] = value
    
    def __getitem__(self, key):
        """
        Return a property of an Element.
        """
        self._raise_not_ready()
        return self._response[key]

    def __iter__(self):
        self._raise_not_ready()
        return self._response.__iter__()
    
    def __len__(self):
        self._raise_not_ready()
        return len(self._response)

    def __contains__(self, item):
        self._raise_not_ready()
        return item in self._response

class Neo4jElementContent(Neo4jContent):
    
    @property
    def _uri(self):
        return self["self"]
    
    @property
    def id(self):
        uri = self._uri
        _id = int(uri.rpartition('/')[-1])
        return _id
    
    @property
    def data(self):
        return self["data"]

    def __response_eq__(self, resp1, resp2):
        return resp1["data"] == resp2["data"]

class Neo4jIndexContent(Neo4jContent):
    
    @property
    def _uri(self):
        tpl = self["template"]
        return tpl.format(key="",value="").rstrip("/")
    
    def __response_eq__(self, resp1, resp2):
        return resp1["template"] == resp2["template"]
    
class Neo4jContentList(Future):
    
    def __init__(self, cid=None, response=None):
        assert cid is not None or response is not None
        self._cid = cid
        self._response = response
        self._filter = filter
    
    class filter_iter(object):
        
        def __init__(self, lst, engine, cls, content_cls, filter):
            self.lst = lst
            self.cls = cls
            self.content_cls = content_cls
            self.engine = engine
            self.filter = filter
        
        def __iter__(self):
            self.lst._raise_not_ready()
            self.it = iter(self.lst._response)
            return self
    
        def next(self):
            n = self.it.next()
            if self.filter(n):
                return self.cls(engine=self.engine,
                                content=self.content_cls(response=n))
    
    def as_elements(self, engine, cls, content_cls=Neo4jContent, filter=lambda x: True):
        return self.filter_iter(self, engine, cls, content_cls, filter)

class Neo4jContentDict(Future):
    
    def __init__(self, cid=None, response=None):
        assert cid is not None or response is not None
        self._cid = cid
        self._response = response
        self._filter = filter
    
    class filter_iter(object):
        
        def __init__(self, lst, engine, cls, content_cls, filter):
            self.lst = lst
            self.cls = cls
            self.content_cls = content_cls
            self.engine = engine
            self.filter = filter
        
        def __iter__(self):
            self.lst._raise_not_ready()
            self.it = self.lst._response.iteritems()
            return self
    
        def next(self):
            k, n = self.it.next()
            if self.filter(n):
                return k, self.cls(engine=self.engine,
                                content=self.content_cls(response=n))
    
    def get_as_element(self, key, engine, cls, content_cls=Neo4jContent, elargs={}):
        n = self._response[key]
        return cls(engine=engine, content=content_cls(response=n), **elargs)
    
    def get_as_content(self, key, content_cls=Neo4jContent):
        n = self._response[key]
        return content_cls(response=n)
    
    def as_elements(self, engine, cls, content_cls=Neo4jContent, filter=lambda x: True):
        return self.filter_iter(self, engine, cls, content_cls, filter)