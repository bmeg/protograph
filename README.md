# protograph

![PROTOGRAPH](https://github.com/bmeg/protograph/blob/master/resources/public/connections.jpg)

# what is protograph?

Protograph is a protocol for describing the transformation from any given schema into a set of graph vertexes and edges. 

Input for Protograph is a stream of messages described in a [Protocol Buffers schema](https://developers.google.com/protocol-buffers/), and a protograph.yaml file that describes how to transform these messages into vertexes and edges.

To a small degree this concerns serialization of the actual values stored in the vertex or edge, but largely Protograph is concerned with how to *link* the separate values together. In order to create a graph out of separate messages, the identifiers in each message that describe their links to other messages must be consistent with one another. Largely this consistency is generated before the messages arrive to be processed by Protograph, but Protograph maintains this consistency and in some cases generates additional references during its processing.

# protograph describes a property graph

To Protograph, vertexes and edges are able to contain properties: ie, key/value pairs which are associated to a given vertex or edge id. These properties are arbitrary structures containing one of these types:

* string
* integer
* double
* list of any mixed values
* map of strings to any value (string, number, list or map)

