
package protograph

import (
  "fmt"
  "log"
  "os"
  "strings"
  "bufio"
  "gopkg.in/yaml.v2"
  "encoding/json"
  "io/ioutil"
  "github.com/golang/protobuf/jsonpb"
  ophion "github.com/bmeg/ophion/client/go"
  "github.com/golang/protobuf/ptypes/struct"
  "github.com/golang/protobuf/proto"
  "github.com/cbroglie/mustache"
)


func ReadLines(path string) (chan []byte, error) {
  out := make(chan []byte, 100)
  if file, err := os.Open(path); err == nil {
    go func() {
      reader := bufio.NewReaderSize(file, 102400)
      var isPrefix bool = true
      var err error = nil
      var line, ln []byte
      for err == nil {
        line, isPrefix, err = reader.ReadLine()
        ln = append(ln, line...)
        if !isPrefix {
          out <- ln
          ln = []byte{}
        }
      }
      close(out)
   } ()
   return out, nil
  } else {
    return out, err
  }
}


func ToJSON(pb proto.Message) (string, error) {
  m := jsonpb.Marshaler{}
  return m.MarshalToString(pb)
}

type ProtoGrapher struct {
  Transforms map[string]TransformMessage
}

func Load(path string) (ProtoGrapher, error) {
  source, err := ioutil.ReadFile(path)
	if err != nil {
		return ProtoGrapher{}, fmt.Errorf("Unable to parse file: %s", err)
	}

  doc := make([]map[interface{}]interface{}, 0)
	err = yaml.Unmarshal(source, &doc)
  out := map[string]TransformMessage{}

  for _, i := range(doc) {
    o, err := json.Marshal(mapNormalize(i))
    if err != nil {
      return ProtoGrapher{}, fmt.Errorf("Unable to parse file: %s", err)
    }
    mes := TransformMessage{}
    err = jsonpb.UnmarshalString(string(o), &mes)
    if err != nil {
      return ProtoGrapher{}, fmt.Errorf("Unable to parse file: %s", err)
    }
    out[mes.Label] = mes
  }

  return ProtoGrapher{Transforms:out}, nil
}


func mapNormalize(v interface{}) interface{} {
	if base, ok := v.(map[interface{}]interface{}); ok {
		out := map[string]interface{}{}
		for k, v := range base {
			out[k.(string)] = mapNormalize(v)
		}
		return out
	} else if base, ok := v.([]interface{}); ok {
		out := make([]interface{}, len(base))
		for i, v := range base {
			out[i] = mapNormalize(v)
		}
		return out
	}
	return v
}


func WrapValue(value interface{}) *structpb.Value {
	switch v := value.(type) {
	case string:
		return &structpb.Value{Kind: &structpb.Value_StringValue{v}}
	case int:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{float64(v)}}
	case int64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{float64(v)}}
	case float64:
		return &structpb.Value{Kind: &structpb.Value_NumberValue{float64(v)}}
	case bool:
		return &structpb.Value{Kind: &structpb.Value_BoolValue{v}}
	case *structpb.Value:
		return v
	case []interface{}:
		o := make([]*structpb.Value, len(v))
		for i, k := range v {
			wv := WrapValue(k)
			o[i] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{&structpb.ListValue{Values: o}}}
	case []string:
		o := make([]*structpb.Value, len(v))
		for i, k := range v {
			wv := &structpb.Value{Kind: &structpb.Value_StringValue{k}}
			o[i] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_ListValue{&structpb.ListValue{Values: o}}}
	case map[string]interface{}:
		o := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		for k, v := range v {
			wv := WrapValue(v)
			o.Fields[k] = wv
		}
		return &structpb.Value{Kind: &structpb.Value_StructValue{o}}
	default:
		log.Printf("unknown data type: %T", value)
	}
	return nil
}


func (self *ProtoGrapher) Convert(data map[string]interface{}, label string) []ophion.GraphQuery {
  if class, ok := data["#label"]; !ok && len(label) == 0 {
    fmt.Printf("Not found: %s\n", data)
    return []ophion.GraphQuery{}
  } else {
    if len(label) > 0 {class = label}
    if trans, ok := self.Transforms[class.(string)]; !ok {
      log.Printf("Not Found: %s\n", class.(string))
      return []ophion.GraphQuery{}
    } else {
      gid, _ := mustache.Render(trans.Gid, data)

      buildQueries := []ophion.GraphQuery{}

      vertexCreate := ophion.GraphQuery{
        Query: []*ophion.GraphStatement {
          &ophion.GraphStatement{&ophion.GraphStatement_AddV{AddV:gid}},
        },
      }

      for k, v := range data {
        var found *FieldAction = nil
        for _, i := range trans.Actions {
          if i.Field == k {
            found = i
          }
        }
        if found != nil {
          switch t := found.GetAction().(type) {
          case *FieldAction_SingleEdge:
            log.Printf("Missing SingleEdge %s\n", t)
          case *FieldAction_RepeatedEdges:
            log.Printf("Missing RepeatedEdges %s\n", t)
          case *FieldAction_EmbeddedEdges:
            //log.Printf("Missing EmbeddedEdges %s %s\n", t, v)
            if vlist, ok := v.([]interface{}); ok {
              for _, velm := range vlist {
                if velm_map, ok := velm.(map[string]interface{}); ok {
                  dstKey := velm_map[t.EmbeddedEdges.EmbeddedIn].(string)
                  edgeType := t.EmbeddedEdges.EdgeLabel

                  edge_statments := []*ophion.GraphStatement{
                    &ophion.GraphStatement{&ophion.GraphStatement_V{gid}},
                    &ophion.GraphStatement{&ophion.GraphStatement_AddE{edgeType}},
                    &ophion.GraphStatement{&ophion.GraphStatement_To{dstKey}},
                  }
                  buildQueries = append(buildQueries, ophion.GraphQuery{edge_statments})

                }
              }
            }
          case *FieldAction_RenameProperty:
            log.Printf("Missing RenameProperty %s\n", t)
          case *FieldAction_SerializeField:
            vw := WrapValue( map[string]interface{}{k:v} ).GetStructValue()
            vertexCreate.Query = append(vertexCreate.Query, &ophion.GraphStatement{&ophion.GraphStatement_Property{vw}})
          case *FieldAction_SpliceMap:
            vw := WrapValue( map[string]interface{}{k:v} ).GetStructValue()
            vertexCreate.Query = append(vertexCreate.Query, &ophion.GraphStatement{&ophion.GraphStatement_Property{vw}})
          case *FieldAction_InnerVertex:
            log.Printf("Missing InnerVertex %s %s\n", k, t)
          case *FieldAction_JoinList:
            //log.Printf("Missing JoinList %s %s %s\n", k, t, v)
            if vlist, ok := v.([]interface{}); ok {
              o := make([]string, len(vlist))
              for i := range vlist {
                o[i] = vlist[i].(string)
              }
              sv := strings.Join(o, t.JoinList.Delimiter)
              vw := WrapValue( map[string]interface{}{k:sv} ).GetStructValue()
              vertexCreate.Query = append(vertexCreate.Query,
                &ophion.GraphStatement{&ophion.GraphStatement_Property{vw}},
              )
            }
          case *FieldAction_StoreField:
            log.Printf("Missing StoreField %s\n", t)
          default:
            log.Printf("Unknown %s\n", t)
          }

        } else {
          if k != "#label" {
            vw := WrapValue( map[string]interface{}{k:v} ).GetStructValue()
            vertexCreate.Query = append(vertexCreate.Query, &ophion.GraphStatement{&ophion.GraphStatement_Property{vw}})
          }
        }
      }
      buildQueries = append( []ophion.GraphQuery{vertexCreate}, buildQueries... )
      return buildQueries
    }
  }
}
