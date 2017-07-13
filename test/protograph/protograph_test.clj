(ns protograph.protograph-test
  (:require
   [clojure.test :refer :all]
   [taoensso.timbre :as log]
   [protograph.template :as template]))

(def test-protograph
  {"Yellow"
   [{:out "vertex"
     :gid "yellow:{{id}}"
     :label "Yellow"
     :splice ["info"]
     :properties
     {:primary "primary({{info.base}})"
      :under "{{over}}"}}
    {:out "edge"
     :from-label "Yellow"
     :from "yellow:{{id}}"
     :label "yellowConnects"
     :to-label "Green"
     :to "green:{{greens.id}}"}]

   "Green" []
   "Red" []})

(def bmeg
  (template/load-protograph "resources/config/protograph.yml"))

(deftest template-test
  (testing "protograph template output"
    (let [out (template/process-message test-protograph {:_label "Yellow" :id "obor" :info {:base "c" :under "x"} :over 33333 :greens {:id "thing"}})]
      (log/info out))))

(deftest bmeg-test
  (testing ""))
