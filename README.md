Binary Extractor
=========

Influenced by POF extractors concent introduced in Oracle Coherence,
this library offers framework to build complex extractors/predicates
accessing data in serialized form of object without its deserialization.

Currently only supported serialized form is [Protocol Buffers](https://code.google.com/p/protobuf/).

Framework offers a number of optimization (e.g. elemination of duplicated expression element) 
and utility to build comlex expression (e.g. extraction from map like structure by logical key). 
Framework also allows to combine different binary formats in single expression set (though only on format is implemented so far).


