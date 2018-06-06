(ns protograph.utils
  "Common utilities"
  (:require [clojure.string :as str]))

(defn format-parse-opts
  "pprint parse options"
  [parse-opts]
  (->> parse-opts
       (map #(str/join " :: " (take 3 %)))
       (str/join "\n")))

(defn report-parse-errors
  "pprint parse errors caught by tools.cli validation predicates"
  [errors]
  {:pre [(vector? errors)]}
  (let [error-report
        (str "The following errors occurred while parsing your command:\n"
             (str/join \newline errors))]
    (println error-report)
    (System/exit 1)))
