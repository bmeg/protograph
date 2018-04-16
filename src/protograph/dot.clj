(ns protograph.dot
  "Generate a dot file representing the connections between
  all node types."
  (:require
   [clojure.set :as set]
   [clojure.string :as string]
   [clojure.tools.cli :as cli]
   [protograph
    [utils :as utils]
    [template :as template]]))

(defn- emit-node
  [node]
  (str "    " node " [label=\"" node "\"]"))

(defn- emit-edge
  [{:keys [from label to]}]
  (str "    " from "->" to " [label=\"" label "\"]"))

(defn- emit-dot
  [{:keys [nodes edges]}]
  (let [out-nodes (mapv emit-node nodes)
        out-edges (mapv emit-edge edges)
        header "digraph protograph {"
        footer "}"
        all (reduce into [[header] out-nodes out-edges [footer]])]
    (string/join "\n" all)))

(defn- protograph->graph
  [protograph]
  (let [graph (template/graph-structure protograph)
        nodes (map :gid (:vertexes graph))
        edges (reduce set/union #{} (vals (:from graph)))]
    {:nodes nodes
     :edges edges}))

(def ^:private parse-args
  [["-h" "--help"]
   ["-p" "--protograph PROTOGRAPH" "Path to protograph.yaml"
    :validate [#(and (.exists (java.io.File. %))
                     (-> (java.io.File. %)
                         (.getName)
                         (string/ends-with? ".yaml")))
               "Invalid protograph file"]]
   ["-o" "--output OUTPUT" "prefix for output file"
    :default "protograph.dot"
    :validate [#(not (and (.exists (java.io.File. %))
                          (.isDirectory (java.io.File. %))))
               "Output prefix cannot match existing directory"]]])

(defn -main
  [& args]
  (let [{:keys [errors options]}
        (cli/parse-opts args parse-args)
        {:keys [protograph output help]} options]
    (when help
      (println (utils/format-parse-opts parse-args))
      (System/exit 0))
    (when errors
      (utils/report-parse-errors errors)
      (System/exit 1))
    (when-not protograph
      (println "Please supply a protograph file")
      (System/exit 1))
    (let [protograph (template/load-protograph protograph)
          graph (protograph->graph protograph)
          dot (emit-dot graph)]
      (spit output dot)
      (println (str "File written to " output)))))
