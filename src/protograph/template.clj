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
  (:import
   [java.io StringWriter])
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
  (into
   {}
   (map
    (fn [[k v]]
      [k (f v)]) m)))

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
        [(keyword key) outcome]))
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
  (let [data (merge (:data a) (:data b))
        top (merge a b)
        onto (merge top (evaluate-map edge-fields top))]
    (assoc onto :data data)))

(defn process-entity
  [top-level
   fields
   {:keys
    [state
     data
     splice
     filter
     lookup]
    :as directive}
   entity]
  (let [core (select-keys directive top-level)
        top (evaluate-map core entity)
        data (evaluate-map data entity)
        out (splice-maps data splice entity)
        merged (if (:merge directive)
                 (merge entity out)
                 out)
        slim (apply dissoc merged (map keyword (concat splice filter [:_index :_self])))
        onto (assoc top :data slim)]
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

(defn merge-outcomes
  [outcomes]
  (reduce
   (fn [outcome {:keys [vertexes edges]}]
     (-> outcome 
         (update :vertexes into vertexes)
         (update :edges into edges)))
   {:vertexes []
    :edges []}
   outcomes))

(defn process-index
  [process post {:keys [index] :as directive} message]
  (if (empty? index)
    (process directive (assoc message :_self message))
    (let [series (evaluate-body index message)
          after (mapv
                 (fn [in]
                   (process directive (assoc message :_index in)))
                 series)]
      (post after))))

(defn process-edge
  [protograph message]
  (process-index
   (partial
    process-entity
    [:fromLabel :from :label :toLabel :to]
    edge-fields)
   (partial reduce into [])
   protograph
   message))

(defn process-vertex
  [protograph message]
  (process-index
   (partial
    process-entity
    [:label :gid]
    vertex-fields)
   (partial reduce into [])
   protograph
   message))

  ;; (partial
  ;;  process-index
  ;;  (partial
  ;;   process-entity
  ;;   [:label :gid]
  ;;   vertex-fields))

(declare process-directive)

(defn process-inner
  [protograph state {:keys [path label]} message]
  (if (and path label)
    (let [inner (evaluate-body path message)
          directive (get protograph label)
          directive (assoc directive :state state)]
      (process-directive protograph directive inner))
    []))

(defn process-inner-index
  [protograph state inner message]
  (if inner
    (process-index
     (partial process-inner protograph state)
     merge-outcomes
     inner
     message)
    {:vertexes [] :edges []}))

(defn mapcat
  [f s]
  (reduce into [] (map f s)))

(defn process-directive
  [protograph {:keys [vertexes edges state inner] :as directive} message]
  (let [down (mapv #(process-inner-index protograph state % message) inner)
        down (merge-outcomes down)
        v (mapcat #(process-vertex (assoc % :state state) message) vertexes)
        e (mapcat #(process-edge (assoc % :state state) message) edges)
        v (into (:vertexes down) v)
        e (into (:edges down) e)]
    {:vertexes v
     :edges e}))

(defn process-message
  [{:keys [state] :as protograph} message]
  (let [label (get message :_label)
        directive (get protograph label)]
    (process-directive protograph (assoc directive :state state) message)))

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
       (assoc protograph (:label spec) (select-keys spec [:label :match :vertexes :edges :inner])))
     {} raw)))

(defn protograph->vertexes
  [protograph]
  (reduce
   into []
   (map
    (fn [[message {:keys [vertexes]}]]
      (map
       (fn [{:keys [label]}]
         {:gid label :label label :data {}})
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
         {:from fromLabel :label label :to toLabel :data {}})
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

(defn match-label
  [match message]
  (not
   (empty?
    (filter
     (fn [[k v]]
       (= (get message k) v))
     match))))

(defn match-labels
  [protograph message]
  (let [match (map (juxt :match identity) (vals protograph))
        found (first
               (filter
                (fn [[m p]]
                  (match-label m message))
                match))]
    (get (last found) :label)))

(defn transform-dir-write
  [protograph write {:keys [input label]}]
  (let [state (partial-state)
        labels (map name (keys protograph))]
    (doseq [file (kafka/dir->files input)]
      (log/info file)
      (let [label (or label (kafka/find-label labels (.getName file)))
            lines (line-seq (io/reader file))]
        (doseq [line lines]
          (try
            (let [data (json/parse-string line true)
                  label (or (match-labels protograph data) label)
                  out (process-message
                       (assoc protograph :state state)
                       (assoc data :_label label))]
              (write out))
            (catch Exception e
              (.printStackTrace e)
              (log/info e)
              (log/info line)
              {:vertexes [] :edges []})))))))

(defn write-output
  [prefix entities]
  (let [writer (io/writer (str prefix ".json"))]
    (doseq [entity entities]
      (.write writer (str (json/generate-string entity) "\n")))
    (.close writer)))

(def parse-args
  [["-p" "--protograph PROTOGRAPH" "path to protograph.yaml"
    :default "protograph.yaml"]
   ["-l" "--label LABEL" "label for inputs (if omitted, label is derived from filenames)"]
   ["-i" "--input INPUT" "input file or directory"]
   ["-o" "--output OUTPUT" "prefix for output file"]
   ["-k" "--kafka KAFKA" "host for kafka server"
    :default "localhost:9092"]
   ["-t" "--topic TOPIC" "input topic to read from"]
   ["-x" "--prefix PREFIX" "output topic prefix"]])

(defn write-graph
  [vertex-writer edge-writer {:keys [vertexes edges]}]
  (doseq [vertex vertexes]
    (.write vertex-writer (str (json/generate-string vertex) "\n")))
  (doseq [edge edges]
    (.write edge-writer (str (json/generate-string edge) "\n"))))

(defn converge-writer
  [prefix]
  (let [vertex-writer (io/writer (str prefix ".Vertex.json") :append true)
        edge-writer (io/writer (str prefix ".Edge.json") :append true)]
    {:write (partial write-graph vertex-writer edge-writer)
     :close
     (fn []
       (.close vertex-writer)
       (.close edge-writer))}))

(defn string-writer
  []
  (let [vertex-writer (StringWriter.)
        edge-writer (StringWriter.)]
    {:write (partial write-graph vertex-writer edge-writer)
     :close
     (fn []
       {:vertex (.toString vertex-writer)
        :edge (.toString edge-writer)})}))

(defn -main
  [& args]
  (let [shell (cli/parse-opts args parse-args)
        summary (:summary shell)]
    (try
      (let [env (:options shell)
            protograph (load-protograph (:protograph env))
            output (:output env)
            writer (converge-writer output)]
        (transform-dir-write protograph (:write writer) env)
        ((:close writer)))
      (catch Exception e
        (.printStackTrace e)
        (log/info summary)))))
