# protograph

Transform a stream of messages into a graph

![PROTOGRAPH](https://github.com/bmeg/protograph/blob/master/resources/public/connections.jpg)

# what is protograph?

Protograph is a protocol for transforming messages from any given schema into a set of graph vertexes and edges. 

To do this, you compose a `protograph.yml` describing how to create vertexes and edges given a message of a variety of shapes (called _labels_ in Protograph).

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

* **fromLabel** (the label of the _from_ vertex for the edge)
* **toLabel** (the label of the _to_ vertex for the edge)
* **label** (the label of the edge itself).
* **from** (the gid of the _from_ vertex for the edge)
* **to** (the gid of the _to_ vertex for the edge)
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

To see a larger example, check out the [`protograph.yml` that comes with this repository](https://github.com/bmeg/protograph/blob/master/resources/config/protograph.yml).

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

In general, you specify a transformation for a given message type by describing what the output is going to look like in terms of the input map. This way you can transform messages of any shape or schema into graph elements.

To specify the transformations, you declare what vertexes and edges are generated from a given message label. Each message type can generate any number of vertexes and edges:

    label: Variant
    vertexes:
      - label: Variant
        gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
        merge: true
        filter:
          - sample
    edges:
      - fromLabel: Variant
        toLabel: Biosample
        label: variantInBiosample
        from: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
        to: "{{sample}}"
  
## protograph fields are constructed using selmer templates

Each field in protograph uses a template to construct its final value out of fields in the provided input message. These templates use the double curly brace paradigm to splice values into a larger string. In its simplest form this can literally be splicing a value from the input map directly in:

    # this input
    {value: 5}

    # through this template
    "{{value}}"

    # creates this output
    5

These templates can use dot notation to access into a nested structure:

    # this input
    {outer: {inner: {container: [{jewel: 88888}]}}}

    # through this template
    "extracting the {{outer.inner.container.0.jewel}}"

    # creates this output
    extracting the 88888

There are even simple filters you can trigger using the `|` operator:

    # this input
    {piles: [1, 2, 3, 5, 4, 5, 2, 3, 4, 1]}

    # through this template
    "{{piles|join:!}}"

    # creates this output
    1!2!3!5!4!5!2!3!4!1

There are a variety of filters available. For more information check out the [Selmer documentation](https://github.com/yogthos/Selmer#filters).

## protograph has a protobuffer schema

There is a protobuffer schema for Protograph defined here: [Protograph schema](https://github.com/bmeg/protograph/blob/master/schema/protograph.proto).

# how to write protograph

The overall structure of a `protograph.yml` is a list of transforms indexed by label:

    - label: Variant
      ....

    - label: Gene
      ....

    - label: Biosample
      ....

Everything living under one of these items pertains to input messages with the given label. You can provide this label in two ways. One of which is to put the label under the `_label` key in your input messages (making them self-describing). The other is to use an input stream with a certain naming convention (the label is the last element in the name of the stream). In the latter case all messages in the stream are assumed to have the same label.

When messages are processed, the first thing that happens is the label of the incoming message is matched to one of the protograph transforms. Once a label is chosen, each transform under that label is run on the given message.

Transforms are of two types: transforms that produce vertexes and transforms that produce edges. These live under the `vertexes` and `edges` keys respectively.

    - label: Variant
      vertexes:
        ....
      edges:
        ....

There are many commonalities between creating vertexes and edges, but minor differences as well. As said in the beginning, a vertex has three keys:

* **label** (a string declaring the type of vertex)
* **gid** (a globally unique identifier constructed from the data contained in the message
* **properties** (containing all of the other data)

An edge has six keys: two terminals, a `from` and `to`, each with their own labels:

* **fromLabel** (the label of the _from_ vertex for the edge)
* **toLabel** (the label of the _to_ vertex for the edge)
* **label** (the label of the edge itself).
* **from** (the gid of the _from_ vertex for the edge)
* **to** (the gid of the _to_ vertex for the edge)
* **properties** (once again, the rest of the data is here).

As you can see, both have a `label` and `properties`, but the vertex also defines a unique `gid` while the edge specifies the vertexes it is connected to through `from` and `to`, and the labels of those vertexes with `fromLabel` and `toLabel`.

Each of these fields is constructed from a template as described in the section above `protograph fields are constructed using selmer templates`. Therefore, a vertex transform may look like this:

    - label: Variant
      vertexes:
        - label: Mutation
          gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
          properties:
            alternateBases: "{{alternateBases|join:,}}"

### merge/filter

Sometimes you want all (or most) of the fields present in the input message to appear in the output message, and you don't want to make an entry under `properties` for each one (or maybe you don't even know what all of them are beforehand). This is where `merge` comes in:

    - label: Variant
      gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
      merge: true

Saying `merge: true` will merge all fields from the input message into the output message. If you want all of them _except_ for a handful, you can add a `filter` entry under the `merge`:

    - label: Variant
      gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
      merge: true
      filter:
        - sample

The `filter` is a list of fields to _exclude_ from the merge.

### splice

`splice` is similar to merge, but this time you are splicing in some nested object into the top level. During a `splice` there is no filter step, you just get the whole map at the top level. Like the `filter` directive, `splice` takes a list of paths:

    - label: Variant
      gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
      splice:
        - info
        - center.source

### index

Many times you have an array of things in the incoming message that entail an output of many edges, for instance. Take this example:

    {"name": "azacitidine",
     "smiles": "Nc1ncn([C@@H]2O[C@H](CO)[C@@H](O)[C@H]2O)c(=O)n1",
     "targets": ["DNMT1", "BRAF"],
     ....}

We want to turn everything in the `targets` array into an edge. In cases like these, we can use `_index`!

    vertexes:
      - label: Compound
        gid: "compound:{{name}}"
        merge: true
        filter:
          - targets
    edges:
      - index: targets
        fromLabel: Compound
        toLabel: Gene
        label: targetsGene
        from: "compound:{{name}}"
        to: "gene:{{_index}}"

Notice for the edges, we declare the `index` to be the `targets` field, then later in the `to` field we can reference each item in the `targets` array using `_index`.

The `index` field can also use filters, so say you don't have an array but a comma-separated string:
    
    {"name": "azacitidine",
     "smiles": "Nc1ncn([C@@H]2O[C@H](CO)[C@@H](O)[C@H]2O)c(=O)n1",
     "targets": "DNMT1,BRAF",
     ....}

Insidious! Yet, we can handle this as well using the `split` filter:

    - index: targets|split:,

This makes the edges identical to the previous ones.

# running protograph

You can run Protograph either by transforming a directory containing input messages into Vertex and Edge output files, or by consuming a Kafka topic and emitting to another pair of Kafka topics (one for Vertex and one for Edge).

Either way, start by downloading the [latest release](https://github.com/bmeg/protograph/releases).

## protograph transform with files

To run Protograph on a directory of input files, use the `--input` and `--output` options, along with the path to your `protograph.yml` under `--protograph`:

    java -jar protograph.jar --protograph path/to/protograph.yml --input /path/to/input/messages.Label.json --output /path/to/output/with/file.prefix

Input files must follow a naming convention where the key into the Protograph description is the penultimate element in the file path, so something like

    we.got.these.from.somewhere.Gene.json

This will trigger processing using the Protograph directives under the `label: Gene` heading. Support for multiple path elements or namespaced messages is not currently supported.

Once processing is complete, it will output two files of the form:

    /path/to/output/with/file.prefix.Vertex.json
    /path/to/output/with/file.prefix.Edge.json

depending on what you passed to `--output`.

## protograph transform using kafka

To run Protograph in Kafka mode you must have access to a Kafka node with some topics to import. 

    java -jar protograph.jar --protograph path/to/protograph.yml --topic "topic1 topic2 topic3"

This will by default output to the Kafka topics `protograph.Vertex` and `protograph.Edge`. To change the prefix for these topics pass in something under the `--prefix` key:

    # this will output to the topics inspired.project.Vertex and inspired.project.Edge
    java -jar protograph.jar --protograph path/to/protograph.yml --topic "topic1 topic2 topic3" --prefix inspired.project

If you need to change the kafka host, pass it in under `--kafka`:

    java -jar protograph.jar --protograph path/to/protograph.yml --kafka 10.96.11.82:9092 --topic "topic1 topic2 topic3"