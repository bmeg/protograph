(ns protograph.template
  (:require
   [clojure.string :as string]
   [clojure.pprint :refer [pprint]]
   [clojure.java.io :as io]
   [clojure.tools.cli :as cli]
   [taoensso.timbre :as log]
   [cheshire.core :as json]
   [yaml.core :as yaml]
   [protograph
    [utils :as utils]
    [kafka :as kafka]
    [compile :as compile]
    [validation :as validation]]
   [clojure.spec.alpha :as spec])
  (:import
   [java.io StringWriter])
  (:gen-class))

(spec/check-asserts true)

(defn- convert-int
  [n]
  (if (string? n)
    (try
      (Long/parseLong n)
      (catch Exception e 0))
    n))

(defn- convert-float
  [r]
  (if (string? r)
    (try
      (Double/parseDouble r)
      (catch Exception e 0.0))
    r))

(defn template-or
  [m & is]
  (let [out (mapv (partial get m) is)]
    (first
     (drop-while empty? out))))

(defn truncate
  [s n?]
  (try
    (let [n (Integer/parseInt n?)]
      (if (> (count s) n)
        (.substring s 0 n)
        s))
    (catch Exception e s)))

(def defaults
  {"each" (fn [s k] (mapv #(get % k) s))
   "flatten" flatten
   "split" (fn [s d] (string/split s (re-pattern d)))
   "or" template-or
   "truncate" truncate
   "sort" sort
   "join" (fn [l d] (string/join d l))
   "float" convert-float
   "name" name
   "first" first
   "last" last})

(def ^:private dot-re #"\.")

(defn evaluate-template
  [template context]
  (let [context (merge defaults context)
        f (compile/compile-top template)]
    (f context)))

(defn evaluate-map
  [m context]
  (into
   {}
   (map
    (fn [[k template]]
      (let [press (evaluate-template template context)
            [key type] (string/split (name k) dot-re)
            outcome (condp = type
                      "int" (convert-int press)
                      "float" (convert-float press)
                      press)]
        [(keyword key) outcome]))
    m)))

(defn render-template
  [template context]
  (let [context (merge defaults context)]
    (template context)))

(defn render-map
  [m context]
  (into
   {}
   (map
    (fn [[k template]]
      (let [press
            (try
              (render-template template context)
              (catch Exception e
                (do
                  (log/info "failed" k template)
                  (.printStackTrace e))))
            [key type] (string/split (name k) dot-re)
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
     (merge after (get context splice)))
   before splices))

(def edge-fields
  {:gid (compile/compile-top "({{from}})--{{label}}->({{to}})")})

(def ^:private vertex-fields {})

(defn process-entity
  [top-level
   fields
   {:keys [state data splice filter lookup] :as directive}
   entity]
  (let [core (select-keys directive top-level)
        top (render-map core entity)
        spliced (splice-maps {} splice entity)
        out (merge spliced (render-map data (merge entity spliced)))
        merged (if (:merge directive)
                 (merge entity out)
                 out)
        slim (apply dissoc merged (concat splice filter ["_index" "_self"]))
        onto (assoc top "data" slim)]
    [(merge (render-map fields onto) onto)]))

(defn process-index
  [process post {:keys [label index] :as directive} message]
  (if index
    (if-let [series (render-template index message)]
      (let [after (mapv
                   (fn [in]
                     (process directive (assoc message "_index" in)))
                   series)]
        (post after)))
    (process directive (assoc message "_self" message))))

(defn empty-field?
  [field]
  (or
   (empty? field)
   (= \: (last field))))

(defn empty-edge?
  [edge]
  (or
   (empty-field? (get edge "from"))
   (empty-field? (get edge "to"))))

(defn- process-edge
  [protograph message]
  (remove
   empty-edge?
   (process-index
    (partial
     process-entity
     ;; [:gid :fromLabel :from :label :toLabel :to]
     [:gid :from :label :to]
     edge-fields)
    (partial reduce into [])
    protograph
    message)))

(defn- process-vertex
  [protograph message]
  (process-index
   (partial
    process-entity
    [:label :gid]
    vertex-fields)
   (partial reduce into [])
   protograph
   message))

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

(declare process-directive)

(defn- process-inner
  [protograph state {:keys [path label]} message]
  (if-let [inner (render-template path message)]
    (let [directive (get protograph label)
          directive (assoc directive :state state)]
      (process-directive protograph directive inner))
    []))

(defn- process-inner-index
  [protograph state inner message]
  (if inner
    (process-index
     (partial process-inner protograph state)
     merge-outcomes
     inner
     message)
    {:vertexes [] :edges []}))

(defn- map-cat
  [f s]
  (reduce into [] (map f s)))

(defn- process-directive
  [protograph {:keys [vertexes edges state inner] :as directive} message]
  (let [down (mapv #(process-inner-index protograph state % message) inner)
        down (merge-outcomes down)
        v (map-cat #(process-vertex (assoc % :state state) message) vertexes)
        e (map-cat #(process-edge (assoc % :state state) message) edges)
        v (into (:vertexes down) v)
        e (into (:edges down) e)]
    {:vertexes v
     :edges e}))

(defn process-message
  [{:keys [state] :as protograph} message label]
  (let [directive (get protograph label)]
    (process-directive protograph (assoc directive :state state) message)))

(defn compile-map
  [m]
  (into
   {}
   (map
    (fn [[k v]]
      [k (compile/compile-top v)])
    m)))

(defn compile?
  [template]
  (if template
    (compile/compile-top template)))

(defn compile-edge
  [edge]
  (-> edge
      ;; (update :fromLabel compile/compile-top)
      (update :label compile/compile-top)
      ;; (update :toLabel compile/compile-top)
      (update :from compile/compile-top)
      (update :to compile/compile-top)
      (update :index compile?)
      (update :data compile-map)))

(defn compile-vertex
  [vertex]
  (-> vertex
      (update :label compile/compile-top)
      (update :gid compile/compile-top)
      (update :index compile?)
      (update :data compile-map)))

(defn compile-inner
  [inner]
  (-> inner
      (update :index compile?)
      (update :path compile/compile-top)))

(defn compile-entry
  [entry]
  (-> entry
      (update :inner (partial map compile-inner))
      (update :vertexes (partial map compile-vertex))
      (update :edges (partial map compile-edge))))

(defn compile-protograph
  [entries]
  (map compile-entry entries))

(defn entries->map
  [entries]
  (reduce
   (fn [protograph spec]
     (assoc protograph (:label spec) (select-keys spec [:label :match :vertexes :edges :inner])))
   {} entries))

(defn load-compiled-protograph
  [path]
  (let [raw (->> (slurp path)
                 (yaml/parse-string)
                 (spec/assert :protograph.validation/protograph))
        compiled (compile-protograph raw)]
    (entries->map compiled)))

(defn load-protograph
  [path]
  (let [raw (->> (slurp path)
                 (yaml/parse-string)
                 (spec/assert :protograph.validation/protograph))]
    (entries->map raw)))

(defn- label-map
  [labeled]
  (into {} (map (juxt :label identity) labeled)))

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

(defn- protograph->edges
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

(defn- set-group
  [f s]
  (into {}
        (map (fn [[k v]] [k (set v)])
             (group-by f s))))

(defn graph-structure
  [protograph]
  (let [vertexes (protograph->vertexes protograph)
        edges (protograph->edges protograph)]
    {:vertexes vertexes
     :edges (label-map edges)
     :from (set-group :from edges)
     :to (set-group :to edges)}))

(defn- partial-state
  []
  {:vertexes (atom {})
   :sources (atom {})
   :terminals (atom {})})

(defn match-label
  [match message]
  ((comp not empty?)
   (filter (fn [[k v]] (= (get message k) v))
           match)))

(defn match-labels
  [protograph message]
  (let [match (map (juxt :match identity) (vals protograph))
        found (first
               (filter (fn [[m p]]
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
            (let [data (json/parse-string line)
                  label (or (match-labels protograph data) label)
                  out (process-message
                       (assoc protograph :state state)
                       data
                       label)]
              (write out))
            (catch Exception e
              (.printStackTrace e)
              (log/info e)
              {:vertexes [] :edges []})))))))

(defn- write-graph
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

(def ^:private parse-args
  [["-p" "--protograph PROTOGRAPH" "path to protograph.yaml"
    :validate [#(and (.exists (java.io.File. %))
                     (-> (java.io.File. %)
                         (.getName)
                         (string/ends-with? ".yaml")))
               "Invalid protograph file"]]
   ["-l" "--label LABEL" "label for inputs"]
   ["-i" "--input INPUT" "Input file or directory"
    :validate [#(.exists (java.io.File. %))]]
   ["-o" "--output OUTPUT" "Prefix for output file"]
   ["-k" "--kafka KAFKA" "Host for kafka server"
    :default "localhost:9092"]
   ["-t" "--topic TOPIC" "Input topic to read from"]
   ["-x" "--prefix PREFIX" "Output topic prefix"]
   ["-h" "--help"]])

(defn -main
  [& args]
  (let [{:keys [errors options summary]}
        (cli/parse-opts args parse-args)
        {:keys [input protograph output help]} options]
    (when help
      (println (utils/format-parse-opts parse-args))
      (System/exit 0))
    (when errors
      (utils/report-parse-errors errors)
      (System/exit 1))
    (when-not (and protograph input)
      (println "Missing required args")
      (log/info summary)
      (System/exit 1))
    (try
      (let [protograph (load-compiled-protograph protograph)
            writer (converge-writer output)]
        (transform-dir-write protograph (:write writer) options)
        ((:close writer)))
      (catch Exception e
        (.printStackTrace e)
        (log/info summary)))))

