
package protograph

import (
  "fmt"
  "log"
  "os"
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


func (self *ProtoGrapher) Convert(data map[string]interface{}) []ophion.GraphQuery {
  if class, ok := data["#label"]; !ok {
    fmt.Printf("Not found: %s\n", data)
    return []ophion.GraphQuery{}
  } else {
    if trans, ok := self.Transforms[class.(string)]; !ok {
      fmt.Printf("Not Found: %s\n", class.(string))
      return []ophion.GraphQuery{}
    } else {
      gid, _ := mustache.Render(trans.Gid, data)

      statements := []*ophion.GraphStatement {
        &ophion.GraphStatement{&ophion.GraphStatement_AddV{AddV:gid}},
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

          case *FieldAction_RepeatedEdges:

          case *FieldAction_EmbeddedEdges:

          case *FieldAction_RenameProperty:

          case *FieldAction_SerializeField:
            vw := WrapValue( map[string]interface{}{k:v} ).GetStructValue()
            statements = append(statements, &ophion.GraphStatement{&ophion.GraphStatement_Property{vw}})
          case *FieldAction_SpliceMap:
            vw := WrapValue( map[string]interface{}{k:v} ).GetStructValue()
            statements = append(statements, &ophion.GraphStatement{&ophion.GraphStatement_Property{vw}})
          case *FieldAction_InnerVertex:

          case *FieldAction_JoinList:

          case *FieldAction_StoreField:
          default:
            log.Printf("Unknown %s\n", t)
          }

        } else {
          if k != "#label" {
            vw := WrapValue( map[string]interface{}{k:v} ).GetStructValue()
            statements = append(statements, &ophion.GraphStatement{&ophion.GraphStatement_Property{vw}})
          }
        }
      }
      out := ophion.GraphQuery{
        Query: statements,
      }
      return []ophion.GraphQuery{out}
    }
  }
}
