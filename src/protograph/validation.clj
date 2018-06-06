(ns protograph.validation
  (:require
   [clojure.spec.alpha :as spec]
   [clojure.future :refer [boolean?]]
   [yaml.core :as yaml]
   [clojure.walk :as walk]))

(spec/def ::to
  string?)

(spec/def ::from
  string?)

(spec/def ::toLabel
  string?)

(spec/def ::fromLabel
  string?)

(spec/def ::index
  string?)

(spec/def ::label
  string?)

(spec/def ::path
  string?)

(spec/def ::gid
  string?)

(spec/def ::merge
  boolean?)

(spec/def ::filter
  (spec/coll-of string?))

(spec/def ::fields
  (spec/coll-of string?))

(spec/def ::splice
  (spec/coll-of string?))

(spec/def ::inner
  (spec/coll-of
   (spec/keys :req-un [::index ::path ::label])))

(spec/def ::data
  (spec/map-of keyword? string?))

(spec/def ::edges
  (spec/coll-of
   (spec/keys :req-un [::fromLabel ::toLabel ::label ::from ::to]
              :opt-un [::index ::fields ::filter ::merge])))

(spec/def ::vertexes
  (spec/coll-of
   (spec/keys :req-un [::label ::gid]
              :opt-un [::filter ::fields ::splice ::merge ::data])))

(spec/def ::entry
  (spec/keys :req-un [::label]
             :opt-un [::inner ::edges ::vertexes]))

(spec/def ::protograph
  (spec/coll-of ::entry))
