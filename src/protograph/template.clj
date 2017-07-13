(ns protograph.template
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [taoensso.timbre :as log]
   [selmer.filters :as filters]
   [selmer.parser :as template]
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
  [edge]
  (cond
    (:to edge) partial-sources
    (:from edge) partial-terminals
    :else partial-nodes))

(defn store-partials
  [edge]
  (cond
    (:to edge) partial-terminals
    (:from edge) partial-sources
    :else partial-nodes))

(defn merge-edges
  [a b]
  (let [properties (merge (:properties a) (:properties b))
        out (merge a b)]
    (assoc out :properties properties)))

(defn process-entity
  [top-level fields {:keys [properties splice filter lookup] :as directive} entity]
  (let [core (select-keys directive top-level)
        top (evaluate-map core entity)
        properties (evaluate-map properties entity)
        out (splice-maps properties splice entity)
        merged (if (:merge directive)
                 (merge entity out)
                 out)
        slim (apply dissoc merged (map keyword (concat splice filter)))
        onto (merge (evaluate-map fields top) top)
        out (assoc onto :properties slim)]
    (if lookup
      (let [look (evaluate-template lookup entity)
            partials (lookup-partials out)
            store (store-partials out)]
        (if-let [found (get @partials look)]
          (mapv #(merge-edges % out) found)
          (do
            (swap! store update look conj out)
            [])))
      [out])))

(defn process-index
  [top-level fields {:keys [index] :as directive} entity]
  (if (empty? index)
    (process-entity top-level fields directive entity)
    (let [path (parse-index index)
          series (get-in entity path)]
      (mapv
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
  [{:keys [nodes edges] :as protonode} message]
  {:nodes (mapcat #(process-vertex % message) nodes)
   :edges (mapcat #(process-edge % message) edges)})

(defn process-message
  [protograph message]
  (let [label (get message :_label)
        directive (get protograph label)]
    (process-directive directive message)))

(filters/add-filter! :each (fn [s k] (mapv #(get % (keyword k)) s)))
(filters/add-filter! :split (fn [s d] (string/split s (re-pattern d))))

(defn load-protograph
  [path]
  (let [raw (yaml/parse-string (slurp path))]
    (reduce
     (fn [protograph spec]
       (assoc protograph (:label spec) (select-keys spec [:nodes :edges])))
     {} raw)))

(defn transform-dir
  [protograph path]
  (reduce
   (fn [so file]
     (let [label (kafka/path->label (.getName file))
           lines (line-seq (io/reader file))]
       (reduce
        (fn [so line]
          (try
            (let [data (json/parse-string line true)
                  out (process-message protograph (assoc data :_label label))]
              (merge-with concat so out))
            (catch Exception e
              (.printStackTrace e)
              (log/info e)
              (log/info line)
              so)))
        so lines)))
   {} (kafka/dir->files path)))
