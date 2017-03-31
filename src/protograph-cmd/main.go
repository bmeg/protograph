
package main

import (
  //"io"
  "flag"
  "log"
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

  var client ophion.QueryClient = nil
  if *server_p != "" {
    client, err = ophion.OphionConnect(*server_p)
    if err != nil {
      fmt.Fprintf(os.Stderr, "Error opening connection: %s\n", err)
      os.Exit(1)
    }
    log.Printf("Connected to %s", *server_p)
  }
  lines, err := protograph.ReadLines(dataFile)
  if err != nil {
    log.Printf("%s", err)
    os.Exit(1)
  }
  count := 0
  ctx := context.TODO()
  for l := range lines {
    mes := map[string]interface{}{}
    err = json.Unmarshal(l, &mes)
    if err == nil {
      o := pg.Convert(mes, *class_p)
      for _, i := range o {
        if client != nil {
          stream, err := client.Traversal(ctx, &i )
          if err != nil {
            log.Printf("%s", err)
          } else {
            for _, err := stream.Recv() ; err == nil; _, err = stream.Recv() {
              //log.Printf("msg: %s", msg)
            }
          }
        } else {
          s, _ := protograph.ToJSON(&i)
          fmt.Printf("%s\n", s)
        }
      }
    }
    //if count % 1000 == 0{
      log.Printf("%d lines processed", count)
    //}
    count++
  }
  
}
