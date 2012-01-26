
from nuevo.ogm.typesystem import Type, TypeSystem
from nuevo.ogm.types import String, UString, Integer, Float, List, Dictionary


Neo4jTypeSystem = TypeSystem({
    String  : Type,
    UString : Type,
    Integer : Type,
    Float   : Type,
    List    : Type,
    Dictionary : Type
})
    
    