
from . import with_engine

@with_engine
def test_create_empty(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)    
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1, "connected_to", v2)

    assert len(e1) == 0, "%d != 0" % len(e1)
    
    assert e1.outV == v1, "%r != %r" % (e1.outV, v1)
    assert e1.inV == v2, "%r != %r" % (e1.inV, v2)

@with_engine
def test_create_empty_by_id(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)    
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1.id, "connected_to", v2.id)

    assert len(e1) == 0, "%d != 0" % len(e1)
    
    assert e1.outV == v1, "%r != %r" % (e1.outV, v1)
    assert e1.inV == v2, "%r != %r" % (e1.inV, v2)

@with_engine
def test_create_full(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)    
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1, "connected_to", v2, p1='caca')

    assert len(e1) == 1, "%d != 0" % len(e1)
    assert 'p1' in e1
    assert e1['p1'] == 'caca'

    assert e1.outV == v1, "%r != %r" % (e1.outV, v1)
    assert e1.inV == v2, "%r != %r" % (e1.inV, v2)

@with_engine
def test_create_full_by_id(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)    
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1.id, "connected_to", v2.id, p1='caca')

    assert len(e1) == 1, "%d != 0" % len(e1)
    assert 'p1' in e1
    assert e1['p1'] == 'caca'

    assert e1.outV == v1, "%r != %r" % (e1.outV, v1)
    assert e1.inV == v2, "%r != %r" % (e1.inV, v2)

@with_engine
def test_create_to_non_existent(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)    
    
    v1 = g.vertices.create()
    
    try:
        e1 = g.edges.create(v1, "connected_to", -1, p1='caca')
        assert False, "Should not be reached!"
    except NotFoundException:
        pass

@with_engine
def test_get_existing(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)    
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1.id, "connected_to", v2.id, p1='caca')
    
    e2 = g.edges.get(e1.id)

    assert e1 == e2

@with_engine
def test_get_non_existing(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)    
        
    try:
        e2 = g.edges.get(-1)
        assert False, "Should not be reached!"
    except NotFoundException:
        pass

@with_engine
def test_update_by_edge(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1.id, "connected_to", v2.id, p1='caca')
    
    g.edges.update(e1, p2='otherstring', p3=123)
    
    assert len(e1) == 2
    
    assert 'p1' not in e1
    
    e2 = g.edges.get(e1.id)
    
    assert 'p1' not in e2
    assert e2['p2'] == 'otherstring'
    assert e2['p3'] == 123

@with_engine
def test_update_by_id(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1.id, "connected_to", v2.id, p1='caca')
    
    g.edges.update(e1.id, p2='otherstring', p3=123)
    
    e2 = g.edges.get(e1.id)
    
    assert 'p1' not in e2
    assert e2['p2'] == 'otherstring'
    assert e2['p3'] == 123

@with_engine
def test_update_by_id_not_exists(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    try:
        g.edges.update(-1, p2='otherstring', p3=123)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_delete_by_edge(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1.id, "connected_to", v2.id, p1='caca')
    
    g.edges.delete(e1)
    
    try:
        e2 = g.edges.get(e1.id)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_delete_by_id(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1.id, "connected_to", v2.id, p1='caca')
    
    g.edges.delete(e1.id)
    
    try:
        e2 = g.edges.get(e1.id)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_delete_by_id_not_exists(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    try:
        g.edges.delete(-1)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

