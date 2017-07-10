(ns protograph.template
  (:require
   [clojure.string :as string]
   [yaml.core :as yaml]))

(defn process-edge
  [{:keys [out from-label from label to-label to splice properties] :as directive} edge])

(defn process-vertex
  [{:keys [out gid label splice properties] :as directive} vertex])

(defn process-entity
  [{:keys [out] :as directive} message]
  (condp = (string/lower-case out)
    "vertex" (process-vertex directive message)
    "edge" (process-edge directive message)))

(defn process-message
  [protograph message]
  (let [label (get message :_label)
        proto (get protograph label)]
    (reduce
     (process-entity))))
