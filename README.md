# protograph

Transform a stream of messages into a graph

![PROTOGRAPH](https://github.com/bmeg/protograph/blob/master/resources/public/connections.jpg)

# what is protograph?

Protograph is a protocol for transforming messages encoded in any given schema into a set of graph vertexes and edges. 

Input for Protograph is a stream of messages described in a [Protocol Buffers schema](https://developers.google.com/protocol-buffers/), and a `protograph.yml` file that describes how to transform these messages into vertexes and edges.

Output is a list of vertexes and edges, in a schema of their own.

To a small degree the `protograph.yml` description concerns serialization of the actual values stored in the vertex or edge, but largely Protograph is concerned with how to *link* the separate entities together. In order to create a graph out of many separate messages, the values in each message that represent their identity and links to other entities must be consistent with one another. Largely this consistency is generated before the messages arrive to be processed by Protograph, but Protograph maintains this consistency and in some cases generates additional references during its processing.

In this README referring to a *link* can mean a vertex connecting through an edge to another vertex, but it could also mean an edge connecting to either terminal vertex or even a partial edge referencing its other half.

## protograph describes a property graph

To Protograph, vertexes and edges are able to contain properties: ie, key/value pairs which are associated to a given vertex or edge. These properties are arbitrary structures containing one of these types:

* string
* number (integers or doubles)
* list of any mixed values
* map of strings to any value (string, number, list or map)

While vertexes and edges can both have properties, they differ in that a vertex just has a `gid` and `label`, whereas an edge contains in addition a gid for both `from` and `to` terminals, as well as the vertex label of each terminal (`fromLabel` and `toLabel`).

    # input to Protograph representing a single Variant
    {"sample": "biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM",
     "referenceName": "1",
     "start": 10521380,
     "end": 10521380,
     "referenceBases": "A",
     "alternateBases": ["-"]}

    # protograph.yml representing the transformation
    - label: Variant
      role: Vertex
      gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
      actions: 
        - field: sample
          remote_edges:
            edge_label: variantInBiosample
            destination_label: Biosample

    # output from Protograph (both a vertex and an edge)
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
     "gid": "(variant:1:10521380:10521380:A:-)->(biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM)",
     "properties": {}}

## protograph works with typed messages

Protograph directives are partitioned by type. When creating a protobuffer schema you declare a series of message types, and in `protograph.yml` you refer to these type names when declaring how each message will be processed. This lives under the `label` key:

    # a typed message
    - label: Variant

## each message has a role

When handling messages as they come in, some of the messages translate directly to vertexes, others are edges. The role of a message is distinct from the label of a message: the label represents the input type, whereas the role reflects its place in the output.

Currently there are two roles, `Vertex` and `Edge`. In the future more may appear, but for now this is sufficient.

    # this message will become a vertex
    role: Vertex

## each message type has a gid

Gids are one of the key concepts of Protograph. A `gid` (global identifier) refers to an identifier that can be entirely constructed *from the message itself*. Each message type declares a gid template that accepts the message as an argument and constructs the gid from values found within.

    # this gid is composed of several properties
    gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"

## messages reference one another through gids

Gids are used to link messages together. Typically a message will contain a gid for another message under some property in a string (for a single link) or list (for a multitude of links). Sometimes these references will be embedded inside an inner map, or list of maps. Protograph enables you to specify references anywhere they may live.

    # this variant came from a sample
    "sample": "biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM"

## transformations are comprised of directives

In general, you specify a transformation for a given message type by describing what to do for each key in the message. Each key can contain any arbitrary data, so directives must also handle a variety of message types. Understanding what directives are available, what they do, and how to invoke them is the bulk of understanding how to use Protograph.

    # how to transform a variant
    actions:
      - field: alternateBases
        join_list:
          delimiter: ","
  
# how to write protograph

Protograph has a protocol buffer schema defined [here](https://github.com/bmeg/protograph/blob/master/src/main/proto/protograph/schema/Protograph.proto).

The outer wrapper is the `TransformMessage`. It contains a `label`, a `role`, a `gid`, and all the `actions` which will be applied to the incoming message. The `label` is simply the type of the incoming message ('Variant' in our examples above). The `role` is the output type, either `Vertex` or `Edge`. The `gid` entry provides a template with which to build a gid from data contained inside the message. These are explained above.

So already, the first section of our Protograph description is complete:

    - label: Variant
      role: Vertex
      gid: "variant:{{referenceName}}:{{start}}:{{end}}:{{referenceBases}}:{{alternateBases}}"
      actions:
        - ...

The real core of the Protograph description live under the `actions` key. Let's take a look at these now.

## remote_edges

This is the most common edge type. When you declare this directive, Protograph will treat all values that live under this key as references to other vertexes. It can be a single value, or it can be a list of values. Each value will be treated as a gid to another vertex.

In order to specify how to handle the `remote_edges`, there are minimum two pieces of information you must provide: the new `edge_label` and the `destination_label` of the referenced vertex. You can do that like this:

    - field: inFamily
      remote_edges:
        edge_label: geneInFamily
        destination_label: GeneFamily

This will take whatever is in the field `inFamily` in the incoming message and create an edge to GeneFamily labeled `geneInFamily` with the source as the incoming message.

Both `edge_label` and `destination_label` are part of a common schema message called `EdgeDescription`. `EdgeDescription` has another optional field, `embedded_in`.

### embedded_in

Many times the referenced gid you are looking for is embedded inside another map in the input message. For this you can specify the `embedded_in` field:

    # gid is embedded inside another map
    "target": [{"geneId": "gene:MDM2"}]

    # direct Protograph to unembed it
    - field: target
      remote_edges:
        edge_label: compoundTargetsGene
        destination_label: Gene
        embedded_in: geneId

## edge_source / edge_terminal

When you have a message that represents an edge, you have to specify which property is the source and which is the terminal. You can do this by declaring the `edge_source` and `edge_terminal`. They also take the form of an `EdgeDescription`, so as long as the `edge_label` matches in each they will be joined into an edge.

    # a GeneSynonym is an edge from a GeneDatabase to a Gene
    - label: GeneSynonym
      role: Edge
      gid: "geneSynonym:{{symbol}}"
      actions:
        - field: inDatabase
          edge_source:
            edge_label: synonymForGene
            destination_label: GeneDatabase
        - field: synonymFor
          edge_terminal:
            edge_label: synonymForGene
            destination_label: Gene

Sometimes you have one half of an edge in one message, and the other half in another. In cases like this, you can specify the `edge_source` in one Protograph type and `edge_terminal` in another. As long as the `edge_label` matches the two messages will be fused into a single output edge.

## link_through

Another common case to deal with is where you have a field that references another message, but that message is just an edge to the ultimate vertex you want to link to. Something like this:

    # a Variant message points to a CallSet, 
    # but that is just an intermediate step to Biosample
    {...
     "calls": [{"callSetId": "callSet:ohsu:prime"}]}

    # the corresponding CallSet message, with the
    # Biosample reference we are looking for
    {"gid": "callSet:ohsu:prime",
     "location": "ohsu",
     "method": "prime",
     "biosampleId": "biosample:prime:TCGA-101010"}

This is a case for `link_through`, and its associate `edge_terminal`. The idea is to specify the link you want in the `link_through`, and in the Protograph description for the message you are "going through" you specify how to create the `edge_terminal`:

    # link_through in Variant
    - field: calls
      link_through:
        edge_label: variantInBiosample
        destination_label: Biosample
        embedded_in: callSetId

    # edge_terminal in CallSet
    - field: biosampleId
      edge_terminal:
        edge_label: variantInBiosample
        destination_label: Biosample

If you notice, they take the same arguments and values for those arguments. As long as `edge_label` and `destination_label` match for both directives, Protograph will fuse them into a single edge, and place the properties from the intermediate message into the edge:

    # the output Variant vertex
    {"label": "Variant"
     "gid": "variant:1:10521380:10521380:A:-"
     "properties": {
       "referenceName": "1",
       "start": "10521380",
       "end": "10521380",
       "referenceBases": "A",
       "alternateBases": ["-"]}}}

    # the output edge to Biosample containing the CallSet properties
    {"label": "variantInBiosample",
     "fromLabel": "Variant",
     "from": "variant:1:10521380:10521380:A:-"
     "toLabel": "Biosample",
     "to": "biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM",
     "gid": "(variant:1:10521380:10521380:A:-)->(biosample:CCLE:1321N1_CENTRAL_NERVOUS_SYSTEM)",
     "properties": {
       "location": "ohsu",
       "method": "prime"}}

## inner_vertex

There are times when a message contains another vertex inside of it. It is natural to make an edge from the outer vertex to the inner one, which you can declare with `inner_vertex`:

    - label: Biosample
      role: Vertex
      gid: "biosample:{{datasetId}}:{{name}}"
      actions:
        - field: disease
          inner_vertex:
            edge_label: termForDisease
            destination_label: OntologyTerm
            outer_id: biosampleId

Now, when Protograph receives a message of the type Biosample, it will pull out the map under the `disease` key and pass it to the Protograph description for OntologyTerm.

In this way input messages are not one to one with output vertexes and edges. One message can produce many vertexes and edges, or a single edge or vertex can emerge from many separate messages.