package protograph

import protograph.mustache.Mustache
import protograph.schema.Protograph._
import FieldAction.Action

import java.io.FileInputStream
import scala.collection.mutable
import collection.JavaConverters._

import org.yaml.snakeyaml.Yaml
import com.fasterxml.jackson.core.{JsonGenerator}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import com.trueaccord.scalapb.json.JsonFormat

case class Vertex(gid: String, label: String, properties: Map[String, Any] = Map())
case class Edge(in: String, label: String, out: String, properties: Map[String, Any] = Map())

case class Graph(vertexes: Map[String, Vertex] = Map(), in: Map[String, Seq[Edge]] = Map(), out: Map[String, Seq[Edge]] = Map()) {
  def incoming(point: String): Seq[Vertex] = {
    in(point).map(i => vertexes(i.out))
  }

  def outgoing(point: String): Seq[Vertex] = {
    out(point).map(o => vertexes(o.in))
  }
}

case class EdgeMaps(in: Map[String, List[Edge]], out: Map[String, List[Edge]])
object EdgeMaps {
  def empty: EdgeMaps = {
    new EdgeMaps(Map[String, List[Edge]](), Map[String, List[Edge]]())
  }
}

object Graph {
  def assemble(vertexes: List[Vertex], edges: List[Edge]): Graph = {
    val vertexMap = vertexes.foldLeft(Map[String, Vertex]()) { (m, vertex) =>
      m + (vertex.gid -> vertex)
    }

    val edgeMaps = edges.foldLeft(EdgeMaps.empty) { (edges, edge) =>
      val incoming: List[Edge] = edge :: edges.in.getOrElse(edge.out, List())
      val outgoing: List[Edge] = edge :: edges.out.getOrElse(edge.in, List())
      new EdgeMaps(edges.in + (edge.out -> incoming), edges.out + (edge.in -> outgoing))
    }

    Graph(vertexMap, edgeMaps.in, edgeMaps.out)
  }
}

case class ProtoVertex(
  label: String,
  gid: String,
  properties: Map[String, Any] = Map[String, Any]()
)

case class ProtoEdge(
  label: String,
  fromLabel: String,
  toLabel: String,
  from: String,
  to: String,
  properties: Map[String, Any] = Map[String, Any]()
)

case class Source(
  label:  String,
  fromLabel:  String,
  from:       String,
  properties: Map[String, Any] = Map[String, Any]()
)

case class Terminal(
  label:  String,
  toLabel:  String,
  to:       String,
  properties: Map[String, Any] = Map[String, Any]()
)

class GidTemplate(template: String) extends Mustache(template) {
  def join(l: List[Any]): String = {
    println("joining " + l.toString)
    l.map(_.toString).mkString(",")
  }
}

case class ProtographTransform(transform: TransformMessage, template: GidTemplate) {
  def gid(data: Map[String, Any]): String = {
    data.get("gid").getOrElse {
      template.render(data)
    }.asInstanceOf[String]
  }
}

object ProtographTransform {
  def toProtograph(transform: TransformMessage): ProtographTransform = {
    ProtographTransform(transform, new GidTemplate(transform.gid))
  }
}

trait ProtographEmitter {
  def emitVertex(vertex: ProtoVertex)
  def emitEdge(edge: ProtoEdge)
}

case class Protograph(transforms: Seq[TransformMessage]) {
  val transformMap = transforms.map(step => (step.label, ProtographTransform.toProtograph(step))).toMap
  val default = TransformMessage(label = "default", gid = "default:{{gid}}")
  val defaultTransform = ProtographTransform.toProtograph(default)

  val partialSources = collection.mutable.Map[String, List[Source]]()
  val partialTerminals = collection.mutable.Map[String, List[Terminal]]()

  def addPartialSource(gid: String) (edge: Source): Unit = {
    val here = edge +: partialSources.getOrElse(gid, List[Source]())
    partialSources += (gid -> here)
  }

  def addPartialTerminal(gid: String) (edge: Terminal): Unit = {
    val here = edge +: partialTerminals.getOrElse(gid, List[Terminal]())
    partialTerminals += (gid -> here)
  }

  val printEmitter = new ProtographEmitter {
    def emitVertex(vertex: ProtoVertex) {
      println(vertex)
    }

    def emitEdge(edge: ProtoEdge) {
      println(edge)
    }
  }

  def transformFor(label: String): ProtographTransform = {
    transformMap.getOrElse(label, defaultTransform)
  }

  def graphStructure: Graph = {
    val emptyGraph = (List[Vertex](), List[Edge]())
    val sources = collection.mutable.Map[String, String]()
    val terminals = collection.mutable.Map[String, String]()

    val (vertexes, edges) = transforms.foldLeft(emptyGraph) { (nodes, transform) =>
      nodes match {
        case (vertexes, previousEdges) =>
          val gid = transform.label
          val vertex = Vertex(gid=gid, label=transform.label)
          val edges = transform.actions.foldLeft(List[Edge]()) { (edges, action) =>
            action.action match {
              case Action.RemoteEdges(edge) =>
                Edge(label=edge.edgeLabel, in=gid, out=edge.destinationLabel) :: edges
              case Action.LinkThrough(edge) =>
                Edge(label=edge.edgeLabel, in=gid, out=edge.destinationLabel) :: edges
              case Action.EdgeSource(edge) =>
                val existing = terminals.get(edge.edgeLabel)
                if (existing.isEmpty) {
                  sources += (edge.edgeLabel -> edge.destinationLabel)
                  edges
                } else {
                  Edge(label=edge.edgeLabel, in=edge.destinationLabel, out=existing.get) :: edges
                }
              case Action.EdgeTerminal(edge) =>
                val existing = sources.get(edge.edgeLabel)
                if (existing.isEmpty) {
                  terminals += (edge.edgeLabel -> edge.destinationLabel)
                  edges
                } else {
                  Edge(label=edge.edgeLabel, in=existing.get, out=edge.destinationLabel) :: edges
                }
              case Action.EmbeddedTerminals(edge) =>
                val existing = sources.get(edge.edgeLabel)
                if (existing.isEmpty) {
                  terminals += (edge.edgeLabel -> edge.destinationLabel)
                  edges
                } else {
                  Edge(label=edge.edgeLabel, in=existing.get, out=edge.destinationLabel) :: edges
                }
              case Action.InnerVertex(edge) =>
                Edge(label=edge.edgeLabel, in=gid, out=edge.destinationLabel) :: edges
              case _ => edges
            }
          }

          val newVertexes = if (transform.role == "Vertex") {
            vertex :: vertexes
          } else {
            vertexes
          }

          (newVertexes, edges ++ previousEdges)
      }
    }

    Graph.assemble(vertexes, edges)
  }

  def ensureSeq(x: Any): Seq[Any] = x match {
    case x: Seq[_] => x
    case _ => List(x)
  }

  def unembed(field: Any, embeddedIn: String): Option[String] = {
    if (embeddedIn.isEmpty) {
      Some(field.asInstanceOf[String])
    } else {
      field.asInstanceOf[Map[String, String]].get(embeddedIn)
    }
  }

  def associateEdges(emit: ProtographEmitter) (proto: EdgeDescription) (vertex: ProtoVertex) (field: Option[Any]): Map[String, Any] = {
    field.map { remote =>
      ensureSeq(remote).map { remote =>
        unembed(remote, proto.embeddedIn).map { in =>
          val edge = ProtoEdge(
            label = proto.edgeLabel,
            fromLabel = vertex.label,
            toLabel = proto.destinationLabel,
            from = vertex.gid,
            to = in
          )

          emit.emitEdge(edge)
        }
      }
    }

    Map[String, Any]()
  }

  def linkThrough(emit: ProtographEmitter) (proto: EdgeDescription) (vertex: ProtoVertex) (field: Option[Any]): Map[String, Any] = {
    field.map { through =>
      ensureSeq(through).map { through =>
        unembed(through, proto.embeddedIn).map { through =>
          val key = proto.edgeLabel + through
          val existing = partialTerminals.getOrElse(key, List[Terminal]())
          if (existing.isEmpty) {
            val partial = Source(
              label = proto.edgeLabel,
              fromLabel = vertex.label,
              from = vertex.gid
            )

            // println("partialEdge", proto.edgeLabel, through, proto.destinationLabel)
            addPartialSource(key) (partial)
          } else {
            existing.foreach { exist =>
              val edge = ProtoEdge(
                label = proto.edgeLabel,
                fromLabel = vertex.label,
                toLabel = proto.destinationLabel,
                from = vertex.gid,
                to = exist.to,
                properties = exist.properties
              )

              emit.emitEdge(edge)

              // println("linkThrough", vertex.gid, proto.edgeLabel, proto.destinationLabel, exist.to.get, exist.properties)
            }
          }
        }
      }
    }

    Map[String, Any]()
  }

  def edgeSource(emit: ProtographEmitter) (proto: EdgeDescription) (gid: String) (field: Option[Any]) (data: Map[String, Any]): Map[String, Any] = {
    field.map { source =>
      val key = proto.edgeLabel + gid // source.asInstanceOf[String]
      val existing = partialTerminals.getOrElse(key, List[Terminal]())
      ensureSeq(source).map { source =>
        val sourceString = source.asInstanceOf[String]
        if (existing.isEmpty) {
          // println(proto.edgeLabel, sourceString, proto.destinationLabel)

          val partial = Source(
            label = proto.edgeLabel,
            fromLabel = proto.destinationLabel,
            from = sourceString,
            properties = data
          )

          addPartialSource(key) (partial)
        } else {
          existing.map { exist =>
            // println("edgeSource", sourceString, proto.edgeLabel, exist.toLabel, exist.to, exist.properties)

            val edge = ProtoEdge(
              label = proto.edgeLabel,
              fromLabel = proto.destinationLabel,
              toLabel = exist.toLabel,
              from = sourceString,
              to = exist.to,
              properties = exist.properties ++ data
            )

            emit.emitEdge(edge)
          }
        }
      }
    }

    Map[String, Any]()
  }

  def edgeTerminal(emit: ProtographEmitter) (proto: EdgeDescription) (gid: String) (field: Option[Any]) (data: Map[String, Any]): Map[String, Any] = {
    field.map { terminal =>
      val key = proto.edgeLabel + gid
      val existing = partialSources.getOrElse(key, List[Source]())
      ensureSeq(terminal).map { terminal =>
        if (existing.isEmpty) {
          // println("partialEdge", proto.edgeLabel, key, proto.destinationLabel)

          val partial = Terminal(
            label = proto.edgeLabel,
            to = terminal.asInstanceOf[String],
            toLabel = proto.destinationLabel,
            properties = data
          )

          addPartialTerminal(key) (partial)
        } else {
          existing.map { exist =>
            // println("edgeTerminal", exist.from.get, proto.edgeLabel, proto.destinationLabel, terminal)

            val edge = ProtoEdge(
              label = proto.edgeLabel,
              fromLabel = exist.fromLabel,
              toLabel = proto.destinationLabel,
              from = exist.from,
              to = terminal.asInstanceOf[String],
              properties = exist.properties ++ data
            )

            emit.emitEdge(edge)
          }
        }
      }
    }

    Map[String, Any]()
  }

  def embeddedTerminals(emit: ProtographEmitter) (proto: EdgeDescription) (gid: String) (field: Option[Any]) (data: Map[String, Any]): Map[String, Any] = {
    field.map { terminal =>
      val key = proto.edgeLabel + gid
      val existing = partialSources.getOrElse(key, List[Source]())
      ensureSeq(terminal).map { terminal =>
        val terminalMap = terminal.asInstanceOf[Map[String, Any]]
        val lifted = proto.liftFields.foldLeft(Map[String, Any]()) { (outcome, lift) =>
          val inner = terminalMap.get(lift)
          if (!inner.isEmpty) {
            outcome ++ inner.get.asInstanceOf[List[Map[String, Any]]].reduce(_ ++ _)
          } else {
            outcome
          }
        }

        if (existing.isEmpty) {
          terminalMap.get(proto.embeddedIn).map { id =>
            // println("partialEdge", proto.edgeLabel, key, proto.destinationLabel)
            val partial = Terminal(
              label = proto.edgeLabel,
              to = id.asInstanceOf[String],
              toLabel = proto.destinationLabel,
              properties = (data ++ terminalMap ++ lifted) -- proto.liftFields
            )

            addPartialTerminal(key) (partial)
          }
        } else {
          existing.foreach { exist =>
            terminalMap.get(proto.embeddedIn).map { id =>
              // println("exist", exist)
              // println("edgeTerminal", exist.from, proto.edgeLabel, proto.destinationLabel, id.asInstanceOf[String], exist.properties)

              val edge = ProtoEdge(
                label = proto.edgeLabel,
                fromLabel = exist.fromLabel,
                toLabel = proto.destinationLabel,
                from = exist.from,
                to = id.asInstanceOf[String],
                properties = (exist.properties ++ data ++ lifted) -- proto.liftFields
              )

              emit.emitEdge(edge)
            }
          }
        }
      }
    }

    Map[String, Any]()
  }

  def innerVertex(emit: ProtographEmitter) (proto: InnerVertex) (vertex: ProtoVertex) (field: Option[Any]): Map[String, Any] = {
    def extract(nest: Map[String, Any]) {
      val embedded = nest + (proto.outerId -> vertex.gid)
      val inner = processVertex(emit) (proto.destinationLabel) (embedded)
      val edge = ProtoEdge(
        label = proto.edgeLabel,
        fromLabel = vertex.label,
        toLabel = proto.destinationLabel,
        from = vertex.gid,
        to = inner.gid
      )

      emit.emitVertex(vertex)
      emit.emitEdge(edge)
    }

    field.map { nested =>
      nested match {
        case inner: List[Map[String, Any]] => inner.map(extract)
        case inner: Map[String, Any] => extract(inner)
      }
    }

    Map[String, Any]()
  }

  def renameProperty(rename: RenameProperty) (field: Option[Any]): Map[String, Any] = {
    field.map { value =>
      Map[String, Any](rename.rename -> value)
    }.getOrElse(Map[String, Any]())
  }

  def serializeField(map: SerializeField) (field: Option[Any]): Map[String, Any] = {
    field.map { inner =>
      val json = Protograph.mapper.writeValueAsString(inner)
      Map[String, Any](map.serializedName -> json)
    }.getOrElse(Map[String, Any]())
  }

  def spliceMap(map: SpliceMap) (field: Option[Any]): Map[String, Any] = {
    field.map { inner =>
      inner.asInstanceOf[Map[String, Any]].map { pair =>
        val key = map.prefix + "." + pair._1
        val value = if (map.liftFields) {
          pair._2.asInstanceOf[Seq[Any]].headOption.getOrElse { None }
        } else {
          pair._2
        }
        (key -> pair._2)
      }
    }.getOrElse(Map[String, Any]())
  }

  def joinList(list: JoinList) (key: String) (field: Option[Any]): Map[String, Any] = {
    field.map { inner =>
      val join = inner.asInstanceOf[List[Any]].map(_.toString).mkString(list.delimiter)
      Map[String, Any](key -> join)
    }.getOrElse(Map[String, Any]())
  }

  def storeField(store: StoreField) (key: String) (field: Option[Any]): Map[String, Any] = {
    if (store.store) {
      field.map { inner =>
        Map[String, Any](key -> inner)
      }.getOrElse(Map[String, Any]())
    } else {
      Map[String, Any]()
    }
  }

  def processEdge(emit: ProtographEmitter) (label: String) (data: Map[String, Any]): Map[String, Any] = {
    val transform = transformFor(label)
    val gid = transform.gid(data)
    val properties = transform.transform.actions.foldLeft(Map[String, Any]()) { (outcome, action) =>
      val key = action.field
      val field = data.get(key)
      val generated = action.action match {
        case Action.RenameProperty(rename) =>
          renameProperty(rename) (field)
        case Action.SerializeField(map) =>
          serializeField(map) (field)
        case Action.SpliceMap(map) =>
          spliceMap(map) (field)
        case Action.JoinList(join) =>
          joinList(join) (key) (field)
        case Action.StoreField(store) =>
          storeField(store) (key) (field)
        case _ =>
          Map[String, Any]()
      }

      outcome ++ generated
    } + ("gid" -> gid)

    val remaining = transform.transform.actions.map(_.field).foldLeft(data) ((data, field) =>
      data - field
    )

    transform.transform.actions.foldLeft(properties ++ remaining) { (all, action) =>
      val key = action.field
      val field = data.get(key)
      val links = action.action match {
        case Action.EdgeSource(edge) =>
          edgeSource(emit) (edge) (gid) (field) (all)
        case Action.EdgeTerminal(edge) =>
          edgeTerminal(emit) (edge) (gid) (field) (all)
        case Action.EmbeddedTerminals(edge) =>
          embeddedTerminals(emit) (edge) (gid) (field) (all)
        case _ =>
          Map[String, Any]()
      }

      all ++ links
    }
  }

  def processVertex(emit: ProtographEmitter) (label: String) (data: Map[String, Any]): ProtoVertex = {
    val transform = transformFor(label)
    val gid = transform.gid(data)
    val vertex = ProtoVertex(
      label = label,
      gid = gid
    )

    val properties = transform.transform.actions.foldLeft(Map[String, Any]()) { (outcome, action) =>
      val key = action.field
      val field = data.get(key)
      val properties = action.action match {
        case Action.RemoteEdges(remote) =>
          associateEdges(emit) (remote) (vertex) (field)
        case Action.LinkThrough(link) =>
          linkThrough(emit) (link) (vertex) (field)
        case Action.InnerVertex(inner) =>
          innerVertex(emit) (inner) (vertex) (field)
        case Action.RenameProperty(rename) =>
          renameProperty(rename) (field)
        case Action.SerializeField(map) =>
          serializeField(map) (field)
        case Action.SpliceMap(map) =>
          spliceMap(map) (field)
        case Action.JoinList(join) =>
          joinList(join) (key) (field)
        case Action.StoreField(store) =>
          storeField(store) (key) (field)
        case _ =>
          Map[String, Any]()
      }

      outcome ++ properties
    } + ("gid" -> gid)

    val remaining = transform.transform.actions.map(_.field).foldLeft(data) ((data, field) =>
      data - field
    )

    emit.emitVertex(vertex.copy(properties = properties ++ remaining))
    vertex
  }

  def processMessage(emit: ProtographEmitter) (label: String) (data: Map[String, Any]): Unit = {
    transformFor(label).transform.role match {
      case "Vertex" => processVertex(emit) (label) (data)
      case "Edge" => processEdge(emit) (label) (data)
    }
  }
}

class CamelCaseSerializer extends JsonSerializer[String] {
  def capitalize(s: String): String = {
    Character.toUpperCase(s.charAt(0)) + s.substring(1)
  }

  def camelize(s: String): String = {
    val parts = s.split("_").toList
    (parts.head :: parts.tail.map(capitalize)).mkString
  }

  def serialize(value: String, gen: JsonGenerator, serializers: SerializerProvider) {
    gen.writeFieldName(camelize(value))
  }
}

object Protograph {
  val simpleModule: SimpleModule = new SimpleModule();
  simpleModule.addKeySerializer(classOf[String], new CamelCaseSerializer());

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val camelMapper = new ObjectMapper()
  camelMapper.registerModule(simpleModule);

  def parseJSON(message: String): TransformMessage = {
    JsonFormat.fromJsonString[TransformMessage](message)
  }

  def readJSON(message: String): Map[String, Any] = {
    mapper.readValue(message, classOf[Map[String, Any]])
  }

  def writeJSON(message: Any): String = {
    mapper.writeValueAsString(message)
  }

  def load(path: String): List[TransformMessage] = {
    val yaml = new Yaml()
    val obj = yaml.load(new FileInputStream(path)).asInstanceOf[java.util.ArrayList[Any]]
    obj.asScala.toList.map { step =>
      val json = camelMapper.writeValueAsString(step)
      println(json)
      parseJSON(json)
    }
  }

  def loadProtograph(path: String): Protograph = {
    val transforms = load(path)
    new Protograph(transforms)
  }
}
