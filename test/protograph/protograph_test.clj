(ns protograph.protograph-test)

(def test-protograph
  {"Yellow"
   [{:out "Vertex"
     :gid "yellow:{{id}}"
     :label "Yellow"
     :splice ["info"]
     :properties
     {:primary "{{info.first}}"
      :under "{{over}}"}}
    {:out "Edge"
     :from-label "Yellow"
     :from "yellow:{{id}}"
     :label "yellowConnects"
     :to-label "Green"
     :to "green:{{greens.id}}"}]

   "Green" []
   "Red" []})
