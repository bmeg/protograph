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
   [protograph.kafka :as kafka])
  (:gen-class))

(defn convert-int
  [n]
  (try
    (Integer/parseInt n)
    (catch Exception e 0)))

(defn convert-float
  [r]
  (try
    (Double/parseDouble r)
    (catch Exception e 0.0)))

(def defaults
  {})

(defn evaluate-template
  [template context]
  (let [context (merge defaults context)]
    (template/render template context)))

(defn map-values
  [f m]
  (into {} (map (fn [[k v]] [k (f v)]) m)))

(def dot #"\.")

(defn evaluate-map
  [m context]
  (into
   {}
   (map
    (fn [[k template]]
      (let [press (evaluate-template template context)
            [key type] (string/split (name k) dot)
            outcome (condp = type
                      "int" (convert-int press)
                      "float" (convert-float press)
                      press)]
        [key outcome]))
    m)))

(defn splice-maps
  [before splices context]
  (reduce
   (fn [after splice]
     (merge after (get context (keyword splice))))
   before splices))

(def edge-fields
  {:gid "({{from}})--{{label}}->({{to}})"})

(def vertex-fields
  {})

(def partial-vertexes (atom {}))
(def partial-sources (atom {}))
(def partial-terminals (atom {}))

(defn lookup-partials
  [state edge]
  (cond
    (:to edge) (:sources state)
    (:from edge) (:terminals state)
    :else (:vertexes state)))

(defn store-partials
  [state edge]
  (cond
    (:to edge) (:terminals state)
    (:from edge) (:sources state)
    :else (:vertexes state)))

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
        slim (apply dissoc merged (map keyword (concat splice filter [:_index :_self])))
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
    (process-entity top-level fields directive (assoc entity :_self entity))
    (let [series (evaluate-body index entity)]
      (reduce
       into []
       (map
        (fn [in]
          (let [payload (assoc entity :_index in)
                process (process-entity top-level fields directive payload)]
            process))
        series)))))

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
  [{:keys [vertexes edges state embedded protograph]} message]
  {:vertexes (mapcat #(process-vertex (assoc % :state state) message) vertexes)
   :edges (mapcat #(process-edge (assoc % :state state) message) edges)})

(defn process-message
  [{:keys [state] :as protograph} message]
  (let [label (get message :_label)
        directive (get protograph label)]
    (process-directive (assoc directive :state state) message)))

(defn template-or
  [m & is]
  (let [out (mapv #(get m (keyword %)) is)]
    (first
     (drop-while empty? out))))

(filters/add-filter! :each (fn [s k] (mapv #(get % (keyword k)) s)))
(filters/add-filter! :flatten flatten)
(filters/add-filter! :split (fn [s d] (string/split s (re-pattern d))))
(filters/add-filter! :or template-or)
(filters/add-filter! :float convert-float)
(filters/add-filter! :name name)

(defn load-protograph
  [path]
  (let [raw (yaml/parse-string (slurp path))]
    (reduce
     (fn [protograph spec]
       (assoc protograph (:label spec) (select-keys spec [:vertexes :edges])))
     {} raw)))

(defn protograph->vertexes
  [protograph]
  (reduce
   into []
   (map
    (fn [[message {:keys [vertexes]}]]
      (map
       (fn [{:keys [label]}]
         {:gid label :label label :properties {}})
       vertexes))
    protograph)))

(defn protograph->edges
  [protograph]
  (reduce
   into []
   (map
    (fn [[message {:keys [edges]}]]
      (map
       (fn [{:keys [fromLabel label toLabel]}]
         {:from fromLabel :label label :to toLabel :properties {}})
       edges))
    protograph)))

(defn set-group
  [f s]
  (into
   {}
   (map
    (fn [[k v]]
      [k (set v)])
    (group-by f s))))

(defn graph-structure
  [protograph]
  (let [vertexes (protograph->vertexes protograph)
        edges (protograph->edges protograph)
        vertex-map (into {} (map (juxt :label identity) vertexes))]
    {:vertexes vertex-map
     :from (set-group :from edges)
     :to (set-group :to edges)}))

(defn partial-state
  []
  {:vertexes (atom {})
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
                    {:vertexes [] :edges []})))
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
        {:keys [vertexes edges]} (transform-dir protograph (:input env))
        _ (log/info "vertexes" (count vertexes) "edges" (count edges))
        output (:output env)]
    (write-output (str output ".Vertex") vertexes)
    (write-output (str output ".Edge") edges)))
