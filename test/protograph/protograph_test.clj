(ns protograph.protograph-test
  (:require
   [clojure.test :refer :all]
   [taoensso.timbre :as log]
   [protograph.template :as template]))

(def test-protograph
  {"yellow.Yellow"
   {:vertexes
    [{:gid "yellow:{{id}}"
      :label "Yellow"
      :splice ["info"]
      :data
      {:primary "primary({{info.base}})"
       :under "{{over}}"}}]
    :edges
    [{:index "greens"
      :from-label "Yellow"
      :from "yellow:{{id}}"
      :label "yellowConnects"
      :to-label "Green"
      :to "green:{{_index.id}}"}]
    :inner
    [{:index "greens"
      :path "_index"
      :label "Green"}]}

   "Green"
   {:vertexes
    [{:gid "green:{{id}}"
      :label "Green"}]
    :inner
    [{:path "orange"
      :label "Orange"}]}

   "Red"
   {}

   "Orange"
   {:vertexes
    [{:gid "orange:{{flail}}"
      :label "Orange"
      :merge true}]}})

(def yellow
  {:_label "yellow.Yellow"
   :id "obor"
   :info {:base "c" :under "x"}
   :over 33333
   :greens
   [{:id "thing"
     :orange {:flail 18181}}]})

(def bmeg
  (template/load-protograph "resources/config/protograph.yml"))

(deftest template-test
  (testing "protograph template output"
    (let [out (template/process-message test-protograph yellow)]
      (log/info out))))

(deftest path-test
  (testing "protograph template output"
    (let [writer (template/string-writer)
          process (template/transform-dir-write test-protograph (:write writer) "resources/test/yellow")
          outcome ((:close writer))]
      (log/info "vertexes" (:vertex outcome))
      (log/info "edges" (:edge outcome)))))

(deftest bmeg-test
  (testing ""))
