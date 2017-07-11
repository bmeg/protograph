(ns protograph.template
  (:require
   [clojure.string :as string]
   [taoensso.timbre :as log]
   [selmer.parser :as template]
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
  (reduce
   (fn [after splice]
     (merge after (get context splice)))
   before splices))

(def edge-fields
  {:gid "({{from}})--{{label}}->({{to}})"})

(def vertex-fields
  {})

(defn process-entity
  [top-level fields {:keys [splice properties] :as directive} entity]
  (let [core (select-keys directive top-level)
        top (evaluate-map core entity)
        properties (evaluate-map properties entity)
        out (splice-maps top splice entity)
        onto (evaluate-map fields out)]
    (assoc (merge onto out) :properties properties)))

(def process-edge
  (partial
   process-entity
   [:from-label :from :label :to-label :to]
   edge-fields))

(def process-vertex
  (partial
   process-entity
   [:label :gid]
   vertex-fields))

(defn process-entity
  [{:keys [out] :as directive} message]
  (condp = (string/lower-case out)
    "vertex" (process-vertex directive message)
    "edge" (process-edge directive message)))

(defn process-message
  [protograph message]
  (let [label (get message :_label)
        proto (get protograph label)]
    (mapv
     (fn [directive]
       (process-entity directive message))
     proto)))
