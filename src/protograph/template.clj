(ns protograph.template
  (:require
   [clojure.string :as string]
   [clojure.pprint :as pprint]
   [clojure.java.io :as io]
   [clojure.tools.cli :as cli]
   [taoensso.timbre :as log]
   [selmer.filters :as filters]
   [selmer.parser :as template]
   [selmer.filter-parser :as parser]
   [cheshire.core :as json]
   [yaml.core :as yaml]
   [protograph.kafka :as kafka]))

(def defaults
  {})

(defn evaluate-template
  [template context]
  (let [context (merge defaults context)]
    (template/render template context)))

(defn map-values
  [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(defn evaluate-map
  [m context]
  (map-values #(evaluate-template % context) m))

(defn splice-maps
  [before splices context]
  (reduce
   (fn [after splice]
     (merge after (get context (keyword splice))))
   before splices))

(defn parse-unit
  [u]
  (try
    (Integer/parseInt u)
    (catch Exception e
      (keyword u))))

(defn parse-index
  [index]
  (let [parts (string/split index #"\.")]
    (map parse-unit parts)))

(def edge-fields
  {:gid "({{from}})--{{label}}->({{to}})"})

(def vertex-fields
  {})

(def partial-nodes (atom {}))
(def partial-sources (atom {}))
(def partial-terminals (atom {}))

(defn lookup-partials
  [state edge]
  (cond
    (:to edge) (:sources state)
    (:from edge) (:terminals state)
    :else (:nodes state)))

(defn store-partials
  [state edge]
  (cond
    (:to edge) (:terminals state)
    (:from edge) (:sources state)
    :else (:nodes state)))

(defn merge-edges
  [a b]
  (let [properties (merge (:properties a) (:properties b))
        top (merge a b)
        onto (merge top (evaluate-map edge-fields top))]
    (assoc onto :properties properties)))

(defn process-entity
  [top-level
   fields
   {:keys
    [state
     properties
     splice
     filter
     lookup]
    :as directive}
   entity]
  (let [core (select-keys directive top-level)
        top (evaluate-map core entity)
        properties (evaluate-map properties entity)
        out (splice-maps properties splice entity)
        merged (if (:merge directive)
                 (merge entity out)
                 out)
        slim (apply dissoc merged (map keyword (concat splice filter)))
        onto (assoc top :properties slim)]
    (if lookup
      (let [look (evaluate-template lookup entity)
            partials (lookup-partials state onto)
            store (store-partials state onto)]
        (if-let [found (get @partials look)]
          (do
            ;; (log/info (:_label entity) look)
            ;; (pprint/pprint found)
            (mapv #(merge-edges % onto) found))
          (do
            ;; (log/info (:_label entity) look)
            ;; (pprint/pprint onto)
            (swap! store update look conj onto)
            [])))
      [(merge (evaluate-map fields onto) onto)])))

(defn evaluate-body
  [template context]
  ((parser/compile-filter-body template false) context))

(defn process-index
  [top-level fields {:keys [index] :as directive} entity]
  (if (empty? index)
    (process-entity top-level fields directive entity)
    (let [;; path (parse-index index)
          ;; series (get-in entity path)
          series (evaluate-body index entity)]
      (log/info index series)
      (mapcat
       (comp
        (partial process-entity top-level fields directive)
        (partial assoc entity :_index))
       series))))

(def process-edge
  (partial
   process-index
   [:fromLabel :from :label :toLabel :to]
   edge-fields))

(def process-vertex
  (partial
   process-index
   [:label :gid]
   vertex-fields))

(defn process-directive
  [{:keys [nodes edges state embedded protograph]} message]
  {:nodes (mapcat #(process-vertex (assoc % :state state) message) nodes)
   :edges (mapcat #(process-edge (assoc % :state state) message) edges)})

(defn process-message
  [{:keys [state] :as protograph} message]
  (let [label (get message :_label)
        directive (get protograph label)]
    (process-directive (assoc directive :state state) message)))

(filters/add-filter! :each (fn [s k] (mapv #(get % (keyword k)) s)))
(filters/add-filter! :flatten flatten)
(filters/add-filter! :split (fn [s d] (string/split s (re-pattern d))))

(defn load-protograph
  [path]
  (let [raw (yaml/parse-string (slurp path))]
    (reduce
     (fn [protograph spec]
       (assoc protograph (:label spec) (select-keys spec [:nodes :edges])))
     {} raw)))

(defn partial-state
  []
  {:nodes (atom {})
   :sources (atom {})
   :terminals (atom {})})

(defn transform-dir
  [protograph path]
  (let [state (partial-state)
        out
        (mapv
         (fn [file]
           (log/info file)
           (let [label (kafka/path->label (.getName file))
                 lines (line-seq (io/reader file))]
             (mapv
              (fn [line]
                (print ".")
                (try
                  (let [data (json/parse-string line true)
                        out (process-message
                             (assoc protograph :state state)
                             (assoc data :_label label))]
                    out)
                  (catch Exception e
                    (.printStackTrace e)
                    (log/info e)
                    (log/info line)
                    {:nodes [] :edges []})))
              lines)))
         (kafka/dir->files path))]
    (apply merge-with into (flatten out))))

(defn write-output
  [prefix entities]
  (let [writer (io/writer (str prefix ".json"))]
    (doseq [entity entities]
      (.write writer (str (json/generate-string entity) "\n")))
    (.close writer)))

(def parse-args
  [["-p" "--protograph PROTOGRAPH" "path to protograph.yml"
    :default "protograph.yml"]
   ["-k" "--kafka KAFKA" "host for kafka server"
    :default "localhost:9092"]
   ["-i" "--input INPUT" "input file or directory"]
   ["-o" "--output OUTPUT" "prefix for output file"]
   ["-t" "--topic TOPIC" "input topic to read from"]
   ["-x" "--prefix PREFIX" "output topic prefix"]])

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))
        protograph (load-protograph (:protograph env))
        {:keys [nodes edges]} (transform-dir protograph (:input env))
        _ (log/info "nodes" (count nodes) "edges" (count edges))
        output (:output env)]
    (write-output (str output ".Vertex") nodes)
    (write-output (str output ".Edge") edges)))
