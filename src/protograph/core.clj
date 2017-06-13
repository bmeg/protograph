(ns protograph.core
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [clojure.tools.cli :as cli]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [protograph.kafka :as kafka])
  (:import
   [protograph Protograph ProtographEmitter]))

(defn load-protograph
  [path]
  (Protograph/loadProtograph path))

(defn build-edge-map
  [edge]
  {"label" (.label edge)
   "fromLabel" (.fromLabel edge)
   "toLabel" (.toLabel edge)
   "from" (.from edge)
   "to" (.to edge)
   "properties" (.properties edge)})

(defn build-edge-gid
  [from label to]
  (str
   "(" from
   ")--" label
   "->(" to ")"))

(defn embed-gid
  [edge]
  (let [out (build-edge-map edge)
        gid (build-edge-gid (get out "from") (get out "label") (get out "to"))]
    (assoc out "gid" gid)))

(defn emitter
  ([emit-vertex emit-edge] (emitter emit-vertex emit-edge (fn [])))
  ([emit-vertex emit-edge close-emitter]
   (reify ProtographEmitter
     (emitVertex [_ vertex]
       (emit-vertex vertex))
     (emitEdge [_ edge]
       (emit-edge
        (embed-gid edge)))
     (close [_]
       (close-emitter)))))

(defn process
  [protograph emit label data]
  (.processMessage protograph emit label data))

(defn kafka-emitter
  [producer vertex-topic edge-topic]
  (emitter
   (fn [vertex]
     (log/info vertex)
     (kafka/send-message producer vertex-topic (Protograph/writeJSON vertex)))
   (fn [edge]
     (log/info edge)
     (kafka/send-message producer edge-topic (Protograph/writeJSON edge)))))

(defn file-emitter
  [vertex-writer edge-writer]
  (emitter
   (fn [vertex]
     (log/info vertex)
     (.write vertex-writer (str (Protograph/writeJSON vertex) "\n")))
   (fn [edge]
     (log/info edge)
     (.write edge-writer (str (Protograph/writeJSON edge) "\n")))
   (fn []
     (.close vertex-writer)
     (.close edge-writer))))

(defn make-kafka-emitter
  [host prefix]
  (let [producer (kafka/producer host)
        vertex-topic (str prefix ".Vertex")
        edge-topic (str prefix ".Edge")]
    (kafka-emitter producer vertex-topic edge-topic)))

(defn make-file-emitter
  [output]
  (let [vertex-out (io/writer (str output ".Vertex.json"))
        edge-out (io/writer (str output ".Edge.json"))]
    (file-emitter vertex-out edge-out)))

(defn transform-message
  [protograph emit message]
  (let [label (kafka/topic->label (.topic message))
        data (Protograph/readJSON (.value message))]
    (process protograph emit label data)))

(defn transform-topics
  [config protograph topics emit]
  (let [host (get-in config [:kafka :host])
        group-id (get-in config [:kafka :consumer :group-id])
        consumer (kafka/consumer {:host host :group-id group-id :topics topics})]
    (kafka/consume
     consumer
     (partial transform-message protograph emit))))

(defn transform-dir
  [config protograph input emit]
  (doseq [file (kafka/dir->files input)]
    (let [label (kafka/path->label (.getName file))]
      (doseq [line (line-seq (io/reader file))]
        (let [data (Protograph/readJSON line)]
          (process protograph emit label data)))))
  (.close emit))

(def default-config
  {:protograph
   {:path "../gaia-bmeg/bmeg.protograph.yml"
    :prefix "protograph.bmeg"}
   :kafka kafka/default-config})

(def bmeg-topics
  {:ccle ["ccle.ga4gh.VariantAnnotation" "ccle.ga4gh.CallSet" "ccle.ResponseCurve" "ccle.Biosample" "ccle.GeneExpression" "ccle.ga4gh.Variant"]
   :cna ["ccle.bmeg.cna.CNASegment" "ccle.bmeg.cna.CNACallSet"]
   :ctdd ["ctdd.json.bmeg.phenotype.ResponseCurve" "ctdd.json.bmeg.phenotype.Compound"]
   :cohort ["ccle.Cohort" "gdc.Cohort"]
   :hugo ["hugo.GeneSynonym" "hugo.Pubmed" "hugo.GeneFamily" "hugo.Gene" "hugo.GeneDatabase"]
   :mc3 ["mc3.ga4gh.VariantAnnotation" "mc3.ga4gh.Variant" "mc3.ga4gh.CallSet"]
   :tcga ["tcga.IndividualCohort" "tcga.Biosample" "tcga.GeneExpression" "tcga.Individual"]})

(def parse-args
  [["-p" "--protograph PROTOGRAPH" "path to protograph.yml"
    :default "protograph.yml"]
   ["-k" "--kafka KAFKA" "host for kafka server"
    :default "localhost:9092"]
   ["-i" "--input INPUT" "input file or directory"]
   ["-o" "--output OUTPUT" "prefix for output file"]
   ["-t" "--topic TOPIC" "input topic to read from"]
   ["-x" "--prefix PREFIX" "output topic prefix"]])

(defn assoc-env
  [config env]
  (-> config
      (assoc-in [:protograph :prefix] (:prefix env))
      (assoc-in [:kafka :host] (:kafka env))
      (assoc :command env)))

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))
        config (assoc-env default-config env)
        protograph (load-protograph
                    (or
                     (:protograph env)
                     (get-in config [:protograph :path])))
        emit (if (:output env)
               (make-file-emitter (:output env))
               (make-kafka-emitter (get-in config [:kafka :host]) (:prefix env)))]
    (log/info config)
    (if (:topic env)
      (let [topics (string/split (:topic env) #" +")]
        (transform-topics config protograph topics emit))
      (transform-dir config protograph (:input env) emit))))
