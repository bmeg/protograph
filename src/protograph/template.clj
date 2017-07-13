(ns protograph.template
  (:require
   [clojure.string :as string]
   [taoensso.timbre :as log]
   [selmer.filters :as filters]
   [selmer.parser :as template]
   [cheshire.core :as json]
   [yaml.core :as yaml]))

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
  (log/info m)
  (log/info context)
  (map-values #(evaluate-template % context) m))

(defn splice-maps
  [before splices context]
  (log/info before splices context)
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

(defn process-entity
  [top-level fields {:keys [properties splice filter] :as directive} entity]
  (let [core (select-keys directive top-level)
        top (evaluate-map core entity)
        properties (evaluate-map properties entity)
        out (splice-maps properties splice entity)
        merged (if (:merge directive)
                 (merge entity out)
                 out)
        slim (apply dissoc merged (concat splice filter))
        onto (evaluate-map fields top)]
    [(assoc (merge onto top) :properties slim)]))

(defn process-index
  [top-level fields {:keys [index] :as directive} entity]
  (if (empty? index)
    (process-entity top-level fields directive entity)
    (let [path (parse-index index)
          series (get-in entity path)]
      (map
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

(filters/add-filter! :split (fn [s d] (string/split s (re-pattern d))))

(defn load-protograph
  [path]
  (let [raw (yaml/parse-string (slurp path))]
    (reduce
     (fn [protograph spec]
       (assoc protograph (:label spec) (select-keys spec [:nodes :edges])))
     {} raw)))
