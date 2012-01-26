
from . import with_engine

@with_engine
def test_create_v(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Vertex)
    
    assert i1

@with_engine
def test_create_e(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Edge
    
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Edge)
    
    assert i1

@with_engine
def test_create_exists_v(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    from nuevo.core.exceptions import ExistsException
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Vertex)
    
    #try:
    #    i2 = g.indices.create("idx1", Vertex)
    #    assert False
    #except ExistsException:
    #    pass


@with_engine
def test_get(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    
    g = Graph(engine)

    i1 = g.indices.create("idx1", Vertex)

    i2 = g.indices.get("idx1", Vertex)
    
    assert i1 == i2

@with_engine
def test_get_not_exists(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)

    try:
        i1 = g.indices.get("lkjdfqlwoncqwnmcqwjfqjwelkjqwljk", Vertex)
        assert False
    except NotFoundException:
        pass

@with_engine
def test_get_create(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)

    #try:
    #    i1 = g.indices.get("testidx", Vertex)
    #    assert False
    #except NotFoundException:
    #    pass
    
    #i1 = g.indices.get_create("testidx", Vertex)
    
    #i2 = g.indices.get("testidx", Vertex)

    #assert i1 == i2

@with_engine
def test_index_delete(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    from nuevo.core.exceptions import NotFoundException
    
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Vertex)
    
    g.indices.delete(i1)
    
    try:
        i1 = g.indices.get("idx1", Vertex)
        assert False
    except NotFoundException:
        pass

@with_engine
def test_index_delete_by_name(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    from nuevo.core.exceptions import NotFoundException
    
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Vertex)
    
    g.indices.delete(i1.name, Vertex)
    
    try:
        i1 = g.indices.get("idx1", Vertex)
        assert False
    except NotFoundException:
        pass

@with_engine
def test_index_delete_not_exists(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    from nuevo.core.exceptions import NotFoundException
    
    #g = Graph(engine)
        
    #try:
    #    g.indices.delete("ilkjahdlkfjalsdkjhfdx1", Vertex)
    #    assert False
    #except NotFoundException:
    #    pass

@with_engine
def test_index_put(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Vertex)
    v1 = g.vertices.create()
    
    i1.put(v1, "kk", "vv")

@with_engine
def test_index_lookup_empty(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Vertex)
    
    nods = list(i1.lookup("kk", "vv"))
    assert nods == []

@with_engine
def test_index_lookup(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.elements import Vertex
    
    g = Graph(engine)
    
    i1 = g.indices.create("idx1", Vertex)
    v1 = g.vertices.create()
    
    i1.put(v1, "kk", "vv")
    
    nods = list(i1.lookup("kk", "vv"))
    assert nods == [v1]
