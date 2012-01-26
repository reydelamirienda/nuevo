
from . import with_engine

@with_engine
def test_create_empty(engine):
    from nuevo.core.graph import Graph
    g = Graph(engine)
    
    v1 = g.vertices.create()
    
    assert isinstance(v1.id, int) 
    assert len(v1) == 0
    
@with_engine
def test_create_full(engine):
    from nuevo.core.graph import Graph
    g = Graph(engine)
    
    v1 = g.vertices.create(p1='string', p2=123)
    
    assert isinstance(v1.id, int)
    assert len(v1) == 2
    assert v1['p1'] == 'string'
    assert v1['p2'] == 123

@with_engine
def test_create_two_equal(engine):
    from nuevo.core.graph import Graph
    g = Graph(engine)
    
    v1 = g.vertices.create(p1='string', p2=123)
    v2 = g.vertices.create(p1='string', p2=123)
    
    assert v1 != v2
    
@with_engine
def test_get_existing(engine):
    from nuevo.core.graph import Graph
    g = Graph(engine)
    
    v1 = g.vertices.create(p1='string', p2=123)
    v2 = g.vertices.get(v1.id)
    
    assert v1 == v2

@with_engine
def test_get_not_exists(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException
    
    g = Graph(engine)
    
    try:
        v1 = g.vertices.get(-1)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_update_by_vertex(engine):
    from nuevo.core.graph import Graph
    g = Graph(engine)
    
    v1 = g.vertices.create(p1='string', p2=123)
    
    g.vertices.update(v1, p3='otherstring', p4=456)
    
    assert len(v1) == 2
    assert 'p1' not in v1
    assert 'p2' not in v1
    assert v1['p3'] == 'otherstring'
    assert v1['p4'] == 456

@with_engine
def test_update_by_id(engine):
    from nuevo.core.graph import Graph
    g = Graph(engine)
    
    v1 = g.vertices.create(p1='string', p2=123)
    
    g.vertices.update(v1.id, p3='otherstring', p4=456)
    
    assert len(v1) == 2
    
    assert 'p3' not in v1
    assert 'p4' not in v1
    
    v2 = g.vertices.get(v1.id)
    
    assert 'p1' not in v2
    assert 'p2' not in v2
    assert v2['p3'] == 'otherstring'
    assert v2['p4'] == 456

@with_engine
def test_update_by_id_not_exists(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException
    
    g = Graph(engine)
    
    try:
        g.vertices.update(-1, p3='otherstring', p4=456)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_delete_by_vertex(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    v1 = g.vertices.create(p1='string', p2=123)
    
    g.vertices.delete(v1)
    
    try:
        v2 = g.vertices.get(v1.id)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_delete_by_id(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    v1 = g.vertices.create(p1='string', p2=123)
    
    g.vertices.delete(v1.id)
    
    try:
        v2 = g.vertices.get(v1.id)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_delete_by_id_not_exists(engine):
    from nuevo.core.graph import Graph
    from nuevo.core.exceptions import NotFoundException

    g = Graph(engine)
    
    try:
        g.vertices.delete(-1)
        assert False, "Should have thrown!"
    except NotFoundException:
        pass

@with_engine
def test_edges_label(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1, "connected_to", v2)
    
    edgs = v1.outE("connected_to")
    assert list(edgs) == [e1]
    
    edgs = v1.inE("connected_to")
    assert list(edgs) == []
    
    edgs = v2.outE("connected_to")
    assert list(edgs) == []
    
    edgs = v2.inE("connected_to")
    assert list(edgs) == [e1]
    
    edgs1 = v1.bothE("connected_to")    
    edgs2 = v2.bothE("connected_to")
    assert list(edgs1) == list(edgs2) == [e1]
    
    
@with_engine
def test_edges_all_labels(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    v3 = g.vertices.create()

    e1 = g.edges.create(v1, "label1", v2)
    e2 = g.edges.create(v2, "label2", v3)

    edgs = v1.outE()
    assert list(edgs) == [e1]
    edgs = v1.inE()
    assert list(edgs) == []
    edgs = v1.bothE()
    assert list(edgs) == [e1]
    
    edgs = v2.outE()
    assert list(edgs) == [e2]
    edgs = v2.inE()
    assert list(edgs) == [e1]
    edgs = v2.bothE()
    assert list(edgs) == [e1, e2]
    
    edgs = v3.outE()
    assert list(edgs) == []
    edgs = v3.inE()
    assert list(edgs) == [e2]
    edgs = v3.bothE()
    assert list(edgs) == [e2]
    

@with_engine
def test_edges_some_labels(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    v3 = g.vertices.create()
    v4 = g.vertices.create()
    v5 = g.vertices.create()

    e1 = g.edges.create(v1, "label1", v3)
    e2 = g.edges.create(v2, "label2", v3)
    e3 = g.edges.create(v3, "label3", v4)
    e4 = g.edges.create(v3, "label4", v5)

    edgs = v3.inE("label1")
    assert list(edgs) == [e1]

    edgs = v3.outE("label3")
    assert list(edgs) == [e3]
    
    edgs = v3.bothE("label2", "label4")
    assert list(edgs) == [e2, e4]

@with_engine
def test_vertices_label(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    
    e1 = g.edges.create(v1, "connected_to", v2)
    
    nods = v1.outV("connected_to")
    assert list(nods) == [v2]
    
    nods = v1.inV("connected_to")
    assert list(nods) == []
    
    nods = v2.outV("connected_to")
    assert list(nods) == []
    
    nods = v2.inV("connected_to")
    assert list(nods) == [v1]
    
    nods1 = v1.bothV("connected_to")    
    nods2 = v2.bothV("connected_to")
    assert list(nods1) == [v2]
    assert list(nods2) == [v1]
    
    
@with_engine
def test_vertices_all_labels(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    v3 = g.vertices.create()

    e1 = g.edges.create(v1, "label1", v2)
    e2 = g.edges.create(v2, "label2", v3)
    
    edgs = v2.outV()
    assert list(edgs) == [v3]
    edgs = v2.inV()
    assert list(edgs) == [v1]
    edgs = v2.bothV()
    assert list(edgs) == [v1, v3]
        

@with_engine
def test_vertices_some_labels(engine):
    from nuevo.core.graph import Graph

    g = Graph(engine)
    
    v1 = g.vertices.create()
    v2 = g.vertices.create()
    v3 = g.vertices.create()
    v4 = g.vertices.create()
    v5 = g.vertices.create()

    e1 = g.edges.create(v1, "label1", v3)
    e2 = g.edges.create(v2, "label2", v3)
    e3 = g.edges.create(v3, "label3", v4)
    e4 = g.edges.create(v3, "label4", v5)

    edgs = v3.inV("label1")
    assert list(edgs) == [v1]

    edgs = v3.outV("label3")
    assert list(edgs) == [v4]
    
    edgs = v3.bothV("label2", "label4")
    assert list(edgs) == [v2, v5]