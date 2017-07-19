# protograph

Transform a stream of messages into a graph

![PROTOGRAPH](https://github.com/bmeg/protograph/blob/master/resources/public/connections.jpg)

# what is protograph?

Protograph is a protocol for transforming messages encoded in any given schema into a set of graph vertexes and edges. 

To do this, you compose a `protograph.yml` describing how to create vertexes and edges given a message of a certain type (called "label" in Protograph).

Given a well-constructed `protograph.yml`, input for Protograph is a stream of messages described in a [Protocol Buffers schema](https://developers.google.com/protocol-buffers/), and the output is a list of vertexes and edges, in a schema of their own.

## protograph describes a property graph

To Protograph, vertexes and edges contain properties: ie, key/value pairs which are associated to a given vertex or edge. These properties are arbitrary structures containing one of these types:

* string
* number (integers or doubles)
* list of any mixed values
* map of strings to any value (string, number, list or map)

A vertex contains three keys:

* **label** (a string declaring the type of vertex)
* **gid** (a globally unique identifier constructed from the data contained in the message
* **properties** (containing all of the other data)

An edge has two terminals, a `from` and `to`, each with their own labels:

* **fromLabel**
* **toLabel**
* **label** (the label of the edge itself).
* **from**
* **to**
* **properties** (once again, the rest of the data is here).

## A basic example

#### input to Protograph representing a single Variant

    {"sample": "biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM",
     "referenceName": "1",
     "start": 10521380,
     "end": 10521380,
     "referenceBases": "A",
     "alternateBases": ["-"]}

#### protograph.yml representing the transformation

    - label: Variant
      gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
      nodes:
        - label: Variant
          merge: true
          filter:
            - sample
      edges:
        - fromLabel: Variant
          toLabel: Biosample
          label: variantInBiosample
          from: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
          to: "{{sample}}"

#### output from Protograph (both a vertex and an edge)

    {"label": "Variant"
     "gid": "variant:1:10521380:10521380:A:-"
     "properties": {
       "referenceName": "1",
       "start": 10521380,
       "end": 10521380,
       "referenceBases": "A",
       "alternateBases": ["-"]}}}

    {"label": "variantInBiosample",
     "fromLabel": "Variant",
     "from": "variant:1:10521380:10521380:A:-"
     "toLabel": "Biosample",
     "to": "biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM",
     "gid": "(variant:1:10521380:10521380:A:-)--variantInBiosample->(biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM)",
     "properties": {}}

## protograph works with typed messages

Protograph directives are partitioned by type. When creating a protobuffer schema you declare a series of message types, and in `protograph.yml` you refer to these type names when declaring how each message will be processed. This lives under the `label` key:

    # a typed message
    - label: Variant

## each message type has a gid

Gids are one of the key concepts of Protograph. A `gid` (global identifier) refers to an identifier that can be entirely constructed *from the message itself*. Each message type declares a gid template that accepts the message as an argument and constructs the gid from values found within.

    # this gid is composed of several properties
    gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"

## messages reference one another through gids

Gids are used to link messages together. Typically a message will contain a gid for another message under some property in a string (for a single link) or list (for a multitude of links). Sometimes these references will be embedded inside an inner map, or list of maps. Protograph enables you to specify references anywhere they may live.

    # this variant came from a sample
    "sample": "biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM"

## protograph transformations describe the construction of vertexes and edges

In general, you specify a transformation for a given message type by describing what the output is going to look like in terms of the input map. This way the input messages can be of any shape or schema.

To specify the transformations, you declare what vertexes and edges are generated from a given message label. Each message type can generate any number of vertexes and edges:

    label: Variant
    vertexes:
      - label: Variant
        merge: true
        filter:
          - sample
    edges:
      - fromLabel: Variant
        toLabel: Biosample
        label: variantInBiosample
        from: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
        to: "{{sample}}"
  
# how to write protograph



# running protograph

You can run Protograph either by transforming a directory containing input messages into Vertex and Edge output files, or by consuming a Kafka topic and emitting to another pair of Kafka topics (one for Vertex and one for Edge).

Either way, start by first installing [Leiningen](https://leiningen.org/), then clone this repo and run either of the below options:

## protograph transform with files

To run Protograph on a directory of input files, use the `--input` and `--output` options:

    lein run --protograph path/to/protograph.yml --input /path/to/input/messages.Label.json --output /path/to/output/with/file.prefix

Input files must follow a naming convention where the key into the Protograph description is the penultimate element in the file path, so something like

    we.got.these.from.somewhere.Gene.json

This will trigger processing using the Protograph directives under the `label: Gene` heading. Support for multiple path elements or namespaced messages is not currently supported.

Once processing is complete, it will output two files of the form:

    /path/to/output/with/file.prefix.Vertex.json
    /path/to/output/with/file.prefix.Edge.json

depending on what you passed to `--output`.

## protograph transform using kafka

To run Protograph in Kafka mode you must have access to a Kafka node with some topics to import. 

    lein run --protograph path/to/protograph.yml --topic "topic1 topic2 topic3"

This will by default output to the Kafka topics `protograph.Vertex` and `protograph.Edge`. To change the prefix for these topics pass in something under the `--prefix` key:

    # this will output to the topics inspired.project.Vertex and inspired.project.Edge
    lein run --protograph path/to/protograph.yml --topic "topic1 topic2 topic3" --prefix inspired.project

If you need to change the kafka host, pass it in under `--kafka`:

    lein run --protograph path/to/protograph.yml --kafka 10.96.11.82:9092 --topic "topic1 topic2 topic3"