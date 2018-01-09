(ns protograph.dot
  (:require
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.tools.cli :as cli]
   [protograph.template :as template]))

(defn emit-node
  [node]
  (str "    " node " [label=\"" node "\"]"))

(defn emit-edge
  [{:keys [from label to]}]
  (str "    " from "->" to " [label=\"" label "\"]"))

(defn emit-dot
  [protograph]
  (let [graph (template/graph-structure protograph)
        nodes (keys (:vertexes graph))
        edges (reduce set/union #{} (vals (:from graph)))
        out-nodes (mapv emit-node nodes)
        out-edges (mapv emit-edge edges)
        header "digraph protograph {"
        footer "}"
        all (reduce into [[header] out-nodes out-edges [footer]])]
    (string/join "\n" all)))

(def parse-args
  [["-p" "--protograph PROTOGRAPH" "path to protograph.yaml"
    :default "protograph.yaml"]
   ["-o" "--output OUTPUT" "prefix for output file"
    :default "protograph.dot"]])

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))
        protograph (template/load-protograph (:protograph env))
        dot (emit-dot protograph)]
    (spit (:output env) dot)))
