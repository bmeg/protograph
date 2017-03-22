
package main

import (
  "flag"
  "os"
  "fmt"
  "protograph"
  "encoding/json"
)

func main() {
  class_p := flag.String("class", "", "Name of data class")

  flag.Parse()

  pgFile := flag.Arg(0)
  dataFile := flag.Arg(1)
  pg, err := protograph.Load(pgFile)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Error parsing file: %s\n", err)
    os.Exit(1)
  }
  lines, _ := protograph.ReadLines(dataFile)

  for l := range lines {
    mes := map[string]interface{}{}
    err = json.Unmarshal(l, &mes)
    if err == nil {
      o := pg.Convert(mes, *class_p)
      for _, i := range o {
        s, _ := protograph.ToJSON(&i)
        fmt.Printf("%s\n", s)
      }
    }
  }

}
