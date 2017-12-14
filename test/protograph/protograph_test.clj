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
    [{:from-label "Yellow"
      :from "yellow:{{id}}"
      :label "yellowConnects"
      :to-label "Green"
      :to "green:{{greens.id}}"}]}

   "Green" {}
   "Red" {}})

(def bmeg
  (template/load-protograph "resources/config/protograph.yml"))

(deftest template-test
  (testing "protograph template output"
    (let [out (template/process-message test-protograph {:_label "yellow.Yellow" :id "obor" :info {:base "c" :under "x"} :over 33333 :greens {:id "thing"}})]
      (log/info out))))

(deftest path-test
  (testing "protograph template output"
    (let [writer (template/string-writer)]
      (template/transform-dir-write test-protograph (:write writer) "resources/test/yellow")
      (log/info "reading test protograph" (:close writer)))))

(deftest bmeg-test
  (testing ""))
