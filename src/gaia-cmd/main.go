
package main

import (
  "os"
  "fmt"
  "gaia/protograph"
  "encoding/json"
)

func main() {
  pgFile := os.Args[1]
  dataFile := os.Args[2]
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
      o := pg.Convert(mes)
      for _, i := range o {
        s, _ := protograph.ToJSON(&i)
        fmt.Printf("%s\n", s)
      }
    }
  }

}
