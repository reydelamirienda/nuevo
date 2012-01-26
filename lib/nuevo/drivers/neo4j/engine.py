
from nuevo.core.engine import Engine
from nuevo.core.elements import Element, Vertex, Edge
from nuevo.core.indices import Index

from nuevo.core.exceptions import NotFoundException

from nuevo.drivers.neo4j.rest import RESTException, Neo4jAtomicREST, Neo4jBatchedREST
from nuevo.drivers.neo4j.commands import Neo4jRESTCommandFactory

from nuevo.drivers.neo4j.content import Neo4jElementContent, Neo4jIndexContent, Neo4jContentList, Neo4jContentDict

from contextlib import contextmanager
from itertools import chain

import urlparse
import sys
import posixpath
def relative_url(target, base):
    base = urlparse.urlparse(base)
    target = urlparse.urlparse(target)
    if base.netloc != target.netloc:
        raise ValueError('target and base netlocs do not match')
    base_dir = '.' + posixpath.dirname(base.path)
    target = '.' + target.path
    return '/' + posixpath.relpath(target, start=base_dir)


class VertexImpl(object):
    """
    Mixin implementation of the Vertex operations for Neo4j.
    """
    
    def out_edges(self, vertex, labels):
        resp = self._get_edges(vertex, labels, "out")
        return resp.as_elements(self, Edge, Neo4jElementContent)

    def in_edges(self, vertex, labels):
        resp = self._get_edges(vertex, labels, "in")
        return resp.as_elements(self, Edge, Neo4jElementContent)

    def both_edges(self, vertex, labels):
        resp = self._get_edges(vertex, labels, "all")
        return resp.as_elements(self, Edge, Neo4jElementContent)

    def out_vertices(self, vertex, labels):
        resp = self._get_vertices(vertex, labels, "out")
        return resp.as_elements(self, Vertex, Neo4jElementContent)
    
    def in_vertices(self, vertex, labels):
        resp = self._get_vertices(vertex, labels, "in")
        return resp.as_elements(self, Vertex, Neo4jElementContent)

    def both_vertices(self, vertex, labels):
        resp = self._get_vertices(vertex, labels, "all")
        return resp.as_elements(self, Vertex, Neo4jElementContent)

    def _get_edges(self, vertex, labels, direction):
        uri, id = self._get_uri_id(vertex)
        
        cmd = self.factory.node_relationships(uri, id, direction, labels)
        resp = self.rest.execute(cmd, Neo4jContentList)
        return resp
    
    def _get_vertices(self, vertex, labels, direction):
        uri, id = self._get_uri_id(vertex)
        #if not labels and direction != "all":
        #    raise NotImplementedError("All labels for a specific direction can't be obtained")
        params = dict(
            relationships = list( dict(direction=direction, type=label) for label in labels ),
            max_depth = 1
        )
        if not labels and direction == "in":
            params['return_filter'] = dict(language='javascript',
                                           body=r"position.length() == 1 && position.startNode().equals(position.lastRelationship().getEndNode())")
        if not labels and direction == "out":
            params['return_filter'] = dict(language='javascript',
                                           body=r"position.length() == 1 && position.startNode().equals(position.lastRelationship().getStartNode())")
        
        cmd = self.factory.traversal(uri, id, "node", params)
        resp = self.rest.execute(cmd, Neo4jContentList)
        return resp
    
class VertexProxyImpl(object):
    """
    Mixin implementation of the VertexProxy operations for Neo4j.
    """
    
    def create_vertex(self, data):
        # remove Nones
        data = self._remove_none(data)
        cmd = self.factory.create_node(data)
        return self.rest.execute(cmd, Neo4jElementContent)
    
    def get_vertex(self, id):
        try:
            cmd = self.factory.get(type="node", id=id)
            return self.rest.execute(cmd, Neo4jElementContent)
        except RESTException as ex:
            if ex.status == 404:
                raise NotFoundException("Can't find vertex %d" % id)
            else:
                raise ex
    
    def update_vertex(self, vertex, data):
        uri, id = self._get_uri_id(vertex)
        data = self._remove_none(data)
        try:
            cmd = self.factory.update(uri, id, "node", data)
            self.rest.execute(cmd)
        
            if isinstance(vertex, Vertex) and vertex._content.ready:
                vertex._content.data.clear()
                vertex._content.data.update(data)
        except RESTException as ex:
            if ex.status == 404:
                raise NotFoundException("Can't find vertex %d" % id)
            else:
                raise ex
    
    def delete_vertex(self, vertex):
        uri, id = self._get_uri_id(vertex)
        try:
            cmd = self.factory.delete(uri, id, "node")
            self.rest.execute(cmd)
        except RESTException as ex:
            if ex.status == 404:
                raise NotFoundException("Can't find vertex %d" % id)
            else:
                raise ex
    
class EdgeImpl(object):
    """
    Mixin implementation of the Edge operations for Neo4j.
    """
    
    def out_vertex(self, edge):
        uri = relative_url(edge._content["start"], self.base_url)
        cmd = self.factory.get(uri=uri)
        return self.rest.execute(cmd, Neo4jElementContent)

    def in_vertex(self, edge):
        uri = relative_url(edge._content["end"], self.base_url)
        cmd = self.factory.get(uri=uri)
        return self.rest.execute(cmd, Neo4jElementContent)

class EdgeProxyImpl(object):
    """
    Mixin implementation of the EdgeProxy operations for Neo4j.
    """
    
    def create_edge(self, outv, label, inv, data):
        fr_uri, fr_id = self._get_uri_id(outv)
        to_uri, to_id = self._get_uri_id(inv)
        # remove Nones
        data = self._remove_none(data)
        args = dict(from_id=fr_id, from_uri=fr_uri,
                    to_id=to_id, to_uri=to_uri,
                    label=label, data=data)
        args = self._remove_none(args)
        try:
            cmd = self.factory.create_relationship(**args)
            return self.rest.execute(cmd, Neo4jElementContent)
        except RESTException as ex:
            if ex.status == 404:
                raise NotFoundException("Can't find origin vertex %r" % outv)
            elif ex.status == 400:
                raise NotFoundException("Can't find destination vertex %r" % inv)
            else:
                raise ex
            
    def get_edge(self, id):
        try:
            cmd = self.factory.get(type="relationship", id=id)
            return self.rest.execute(cmd, Neo4jElementContent)
        except RESTException as ex:
            if ex.status == 404:
                raise NotFoundException("Can't find edge %d" % id)
            else:
                raise ex
    
    def update_edge(self, edge, data):
        uri, id = self._get_uri_id(edge)
        data = self._remove_none(data)
        try:
            cmd = self.factory.update(uri, id, "relationship", data)
            self.rest.execute(cmd)
            
            if isinstance(edge, Edge) and edge._content.ready:
                edge._content.data.clear()
                edge._content.data.update(data)
        except RESTException as ex:
            if ex.status == 404:
                raise NotFoundException("Can't find edge %r" % edge)
            else:
                raise ex
    
    def delete_edge(self, edge):
        uri, id = self._get_uri_id(edge)
        try:
            cmd = self.factory.delete(uri, id, "relationship")
            self.rest.execute(cmd)
        except RESTException as ex:
            if ex.status == 404:
                raise NotFoundException("Can't find edge %r" % edge)
            else:
                raise ex

class IndexProxyImpl(object):

    def index_create(self, name, element_type, config):
        if issubclass(element_type, Vertex):
            cmd = self.factory.create_node_index(name, config)
        elif issubclass(element_type, Edge):
            cmd = self.factory.create_relationship_index(name, config)
        else:
            raise TypeError("Only Vertex or Edge indices are supported")
        resp = self.rest.execute(cmd, Neo4jIndexContent)
        return resp

    def index_get(self, name, element_type):
        if issubclass(element_type, Vertex):
            type = "node"
        elif issubclass(element_type, Edge):
            type = "relationship"
        else:
            raise TypeError("Only Vertex or Edge indices are supported")
        
        try:
            cmd = self.factory.get_indices(type)
            resp = self.rest.execute(cmd, Neo4jContentDict)
            return resp.get_as_content(name, Neo4jIndexContent)
        except KeyError as ex:
            raise NotFoundException("Can't find %s index %s" % (element_type.__name__, name))
    
    def index_get_create(self, name, element_type, config):
        try:
            return self.index_get(name, element_type)
        except Exception:
            return self.index_create(name, element_type, config)
    
    def index_delete(self, index=None, element_type=None):
        if isinstance(index, Index):
            uri = relative_url(index._content.uri, self.base_url)
        else:
            assert element_type
            if issubclass(element_type, Vertex):
                type = "node"
            elif issubclass(element_type, Edge):
                type = "relationship"
            else:
                raise TypeError("Only Vertex or Edge indices are supported")
            uri = "/index/%s/%s" % (type, index)
            
        cmd = self.factory.delete_index(uri)
        self.rest.execute(cmd)

class IndexImpl(object):
    
    def index_put(self, index, element, key, value):
        if not isinstance(element, index.element_type):
            raise TypeError("Element type incompatible with index type")
        index_uri = relative_url(index._content.uri, self.base_url)
        cmd = self.factory.index_element(index_uri, element._content.uri, key, value)
        self.rest.execute(cmd)
    
    def index_lookup(self, index, key, value):
        index_uri = relative_url(index._content.uri, self.base_url)
        cmd = self.factory.index_lookup(index_uri, key, value)
        resp = self.rest.execute(cmd, Neo4jContentList)
        return resp.as_elements(self, index.element_type, Neo4jElementContent)

class QueryImpl(object):
    
    def query_cypher(self, q, column_types=None):
        cmd = self.factory.cypher(q)
    

class Neo4jRESTEngine(Engine,
                      VertexImpl, VertexProxyImpl,
                      EdgeImpl, EdgeProxyImpl,
                      IndexImpl, IndexProxyImpl,
                      QueryImpl):
    
    name = "Neo4jREST"
    
    factory = Neo4jRESTCommandFactory
    
    def __init__(self, protocol, path):
        base_url = "%s:%s/" % (protocol, path)
        self.base_url = base_url
        self.rest     = Neo4jAtomicREST(base_url)
        self.rest_stack = []
    
    def _remove_none(self, data):
        data = dict( (k, data[k]) for k in data if data[k] is not None )
        return data
    
    def _start_batch(self):
        self.rest_stack.append(self.rest)
        self.rest = Neo4jBatchedREST(self.base_url)
    
    def _send_batch(self):
        self.rest.flush()
    
    def _end_batch(self):
        self.rest = self.rest_stack.pop()
    
    def _clear_database_for_testing(self):
        self.factory.delete("/cleandb/secret-key")
    
    @contextmanager
    def transaction(self, nest=True):
        if not self.rest_stack:
            nest = True
        
        if nest:
            self._start_batch()
            try:
                yield self
                self._send_batch()
            finally:
                self._end_batch()
        else:
            yield self
    
    def _get_uri_id(self, element):
        uri = id = None
        if isinstance(element, Element):
            uri = relative_url(element._content.uri, self.base_url)
        elif isinstance(element, int):
            id = element
        else:
            raise TypeError("Element or int required")
        return uri, id
    
    def __getattr__(self, key):
        raise NotImplementedError("%s not supported in the Neo4j engine" % key)

def load():
    return Neo4jRESTEngine