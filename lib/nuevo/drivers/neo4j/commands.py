
import json, re

class Neo4jCommand(object):
    
    def __init__(self, method="GET", resource="/", params=None):
        self.method = method
        self.resource = resource
        self.params = params
    
    def __repr__(self):
        return "%s(%r, %r, %r)" % (self.__class__.__name__, self.method, self.resource, self.params)

class Neo4jBatchedCommand(Neo4jCommand):
    
    def __init__(self, cmd, id):
        self.id = id
        if re.match(r"{[0-9]+}", cmd.resource): 
            resource = cmd.resource
        else:
            resource = cmd.resource
        super(Neo4jBatchedCommand, self).__init__(cmd.method, resource, cmd.params)

class JSONCommandEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Neo4jBatchedCommand):
            return dict(method=o.method, to=o.resource,
                        body=o.params, id=o.id)
        elif isinstance(o, Neo4jCommand):
            return o.params
        else:
            return super(CommandEncoder, self).default(obj)

class Neo4jRESTCommandFactory(object):
    
    @staticmethod
    def batch():
        return Neo4jCommand("POST", "/batch", [])
    
    ### Element Proxies
    @staticmethod
    def create_node(data):
        return Neo4jCommand("POST", "/node", data)
    
    @staticmethod
    def create_relationship(label, data, **kwargs):
        assert 'from_uri' in kwargs or 'from_id' in kwargs, "Must provide from_uri or from_id"
        assert 'to_uri' in kwargs or 'to_id' in kwargs, "Must provide to_uri or to_id"
        assert label is not None and label
        
        resource = '%s/relationships' % kwargs.get('from_uri', '/node/%d' % kwargs.get("from_id", -1))
        to = kwargs.get('to_uri', '/node/%d' % kwargs.get("to_id", -1))
        
        params = dict(to=to, type=label, data=data)
        return Neo4jCommand("POST", resource, params)
        
    @staticmethod
    def get(uri=None, type=None, id=None):
        assert uri is not None or ( type is not None and id is not None )
        if uri:
            return Neo4jCommand("GET", uri)
        else:
            return Neo4jCommand("GET", "/%s/%d" % (type, id))
    
    @staticmethod
    def update(uri=None, id=None, type=None, data=None):
        if uri:
            resource = "%s/properties" % uri 
        elif id is not None and type:
            resource = "/%s/%d/properties" % (type, id)
        else:
            raise ValueError("Need either uri or type/id")
        
        return Neo4jCommand("PUT", resource, data)
    
    @staticmethod
    def delete(uri=None, id=None, type=None):
        if uri:
            resource = uri
        elif id is not None and type:
            resource = "/%s/%d" % (type, id)
        else:
            raise ValueError("Need either uri or type/id")
        
        return Neo4jCommand("DELETE", resource)
    
    # Node
    
    @staticmethod
    def node_relationships(uri=None, id=None, dir="all", labels=None):
        assert uri or id is not None
        if not uri:
            uri = "/node/%d" % id
        if isinstance(labels, str):
            labels = (labels,)
        labels = "&".join(labels)
        resource = "%s/relationships/%s/%s" % (uri, dir, labels)
        return Neo4jCommand("GET", resource)
    
    @staticmethod
    def traversal(start_uri=None, start_id=None, return_type="node", params=None):
        assert start_uri or start_id is not None
        if not start_uri:
            start_uri = "/node/%d" % start_id
        resource = "%s/traverse/%s" % (start_uri, return_type)
        return Neo4jCommand("POST", resource, params)
    
    # Indices
    @staticmethod
    def create_node_index(name, config):
        params = dict(name=name)
        if config:
            params['config'] = config
        return Neo4jCommand("POST", "/index/node", params)

    @staticmethod
    def create_relationship_index(name, config):
        params = dict(name=name)
        if config:
            params['config'] = config
        return Neo4jCommand("POST", "/index/relationship", params)
    
    @staticmethod
    def get_indices(type):
        return Neo4jCommand("GET", "/index/%s" % type)
    
    @staticmethod
    def delete_index(uri):
        return Neo4jCommand("DELETE", uri)
    
    @staticmethod
    def index_element(index_uri, element_uri, key, value):
        return Neo4jCommand("POST", index_uri,
                            dict(key=key, value=value, uri=element_uri))
    
    @staticmethod
    def index_lookup(index_uri, key, value):
        return Neo4jCommand("GET", "%s/%s/%s" % (index_uri, key, value) )
    