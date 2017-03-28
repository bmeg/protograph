
package main

import (
  "log"
  "flag"
  "context"
  "os"
  "fmt"
  "protograph"
  "encoding/json"
  ophion "github.com/bmeg/ophion/client/go"
)

func main() {
  class_p := flag.String("class", "", "Name of data class")
  server_p := flag.String("server", "", "OphionServer")

  flag.Parse()

  pgFile := flag.Arg(0)
  dataFile := flag.Arg(1)
  pg, err := protograph.Load(pgFile)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Error parsing file: %s\n", err)
    os.Exit(1)
  }
  lines, _ := protograph.ReadLines(dataFile)

  var client ophion.QueryClient = nil
  if *server_p != "" {
    client, err = ophion.OphionConnect(*server_p)
    if err != nil {
      fmt.Fprintf(os.Stderr, "Error opening connection: %s\n", err)
      os.Exit(1)
    }
  }
  count := 0
  for l := range lines {
    mes := map[string]interface{}{}
    err = json.Unmarshal(l, &mes)
    if err == nil {
      o := pg.Convert(mes, *class_p)
      for _, i := range o {
        if client != nil {
          client.Traversal(context.TODO(), &i )
        } else {
          s, _ := protograph.ToJSON(&i)
          fmt.Printf("%s\n", s)
        }
      }
      count++
      if (count % 1000 == 0) {
        log.Printf("%d messages", count)
      }
    }
  }

}
