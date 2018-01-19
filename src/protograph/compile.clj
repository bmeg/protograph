(ns protograph.compile
  (:require
   [clojure.string :as string]
   [instaparse.core :as parse]))

(def parser-spec
  "field = constant (template constant)*
   constant = #'[^{}]'*
   template = '{{' expression '}}'
   expression = accessor (pipe function)*
   accessor = index (dot index)*
   index = #'[a-zA-Z0-9_,!@#$%^&*?\\-]'+
   function = index (colon argument)*
   argument = #'[^{}]'*
   colon = ':'
   dot = '.'
   pipe = '|'")

(def field-parser
  (parse/parser parser-spec))

(declare compile-switch)

(defn compile-field
  [terms]
  (map compile-switch terms))

(defn compile-expression
  [terms]
  (let [accessor (compile-switch (first terms))
        functions (filter (fn [i] (= :function (first i))) (rest terms))
        functions (map compile-switch functions)]
    [accessor functions]
    (fn [m]
      (apply comp (reverse (map (fn [f] (partial f m)) functions))))))

(defn compile-accessor
  [terms]
  (let [indexes (filter (fn [i] (= :index (first i))) terms)
        indexes (map compile-switch indexes)]
    [:get-in :m indexes]
    (fn [m] (get-in m indexes))))

(defn compile-function
  [terms]
  (let [index (compile-switch (first terms))
        arguments (filter (fn [i] (= :argument (first i))) terms)
        arguments (map compile-switch arguments)]
    [index arguments]
    (fn [m x]
      (let [f (get m index)]
        (apply f (cons x arguments))))))

(defn compile-switch
  [[key & terms]]
  (condp = key
    :field (compile-field terms)
    :constant (apply str terms)
    :template (compile-switch (first (rest (butlast terms))))
    :expression (compile-expression terms)
    :accessor (compile-accessor terms)
    :function (compile-function terms)
    :index (apply str terms)
    :argument (apply str terms)
    terms))

(defn apply-context
  [m f]
  (if (fn? f)
    (f m)
    f))

(defn compile-top
  [field]
  (let [parse (field-parser field)
        compiled (compile-switch parse)]
    (fn [m]
      (let [applied (map (partial apply-context m) compiled)]
        (apply str applied)))))
