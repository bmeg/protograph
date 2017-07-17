(ns protograph.kafka
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [clojure.tools.cli :as cli]
   [taoensso.timbre :as log]
   [clojurewerkz.propertied.properties :as props])
  (:import
   [java.util Properties UUID]
   [org.apache.kafka.clients.consumer KafkaConsumer ConsumerRecord]
   [org.apache.kafka.clients.producer KafkaProducer Producer ProducerRecord]
   [kafka.utils ZkUtils]
   [kafka.admin AdminUtils]))

(def string-serializer
  {"key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
   "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"})

(def string-deserializer
  {"key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})

(def consumer-defaults
  (merge
   string-deserializer
   {"auto.offset.reset" "earliest"
    "enable.auto.commit" "true"
    "auto.commit.interval.ms" "1000"
    "max.poll.records" "80"
    "max.poll.interval.ms" "30000"
    "request.timeout.ms" "40000"
    "session.timeout.ms" "30000"}))

(defn uuid
  []
  (str (UUID/randomUUID)))

(defn producer
  ([host] (producer host {}))
  ([host props]
   (let [config (merge string-serializer {"bootstrap.servers" host} props)]
     (new KafkaProducer config))))

(defn send-message
  [producer topic message]
  (let [record (new ProducerRecord topic (uuid) message)]
    (.send producer record)))

(defn consumer
  ([] (consumer {}))
  ([{:keys [host group-id topics props]}]
   (let [config (merge
                 consumer-defaults
                 {"bootstrap.servers" (or host "localhost:9092")
                  "group.id" (or group-id (uuid))}
                 props)
         devour (new KafkaConsumer (props/map->properties config))]
     (if-not (empty? topics)
       (.subscribe devour topics))
     devour)))

(defn consumer-empty?
  [in]
  (empty? (.poll in 1000)))

(defn consume
  [in handle-message]
  (loop [records (.poll in 1000)]
    (when-not (empty? records)
      (doseq [record records]
        (handle-message record))
      (recur (.poll in 1000)))))

(defn subscribe
  [in topics]
  (.subscribe in topics))

(defn list-topics
  [in]
  (let [topic-map (into {} (.listTopics in))]
    (keys topic-map)))

(defn zookeeper-utils
  [host]
  (ZkUtils/apply
   (or host "localhost:2181")
   30000 30000
   false))

(defn topic-config
  [zookeeper topic]
  (AdminUtils/fetchEntityConfig zookeeper "topics" topic))

(defn set-topic-config!
  [zookeeper topic config]
  (println "setting topic config for " topic config)
  (let [pre (props/properties->map (topic-config zookeeper topic))
        post (props/map->properties (merge pre config))]
    (println "topic config" topic post)
    (AdminUtils/changeTopicConfig zookeeper topic post)))

(defn topic-empty?
  [config topic]
  (let [in (consumer (assoc config :topics [topic]))
        empty (consumer-empty? in)]
    (.close in)
    empty))

(defn purge-topic!
  [zk config topic]
  (set-topic-config! zk topic {"retention.ms" "1000"})
  (loop [n 0]
    (println "looping!" n)
    (if (topic-empty? config topic)
      (set-topic-config! zk topic {"retention.ms" "-1"})
      (do
        (Thread/sleep 1000)
        (recur (inc n))))))

(defn path->topic
  [path]
  "removes the suffix at the end of a path"
  (let [parts (string/split path #"\.")]
    (string/join "." (butlast parts))))

(defn path->label
  [path]
  "extracts the penultimate element out of a path"
  (let [parts (string/split path #"\.")]
    (->> parts reverse (drop 1) first)))

(defn topic->label
  [topic]
  "returns the suffix at the end of a path"
  (let [parts (string/split topic #"\.")]
    (last parts)))

(defn file->stream
  [out file topic]
  (doseq [line (line-seq (io/reader file))]
    (send-message out topic line)))

(defn dir->files
  [path]
  (filter
   #(.isFile %)
   (file-seq (io/file path))))

(defn dir->streams
  [out path]
  (let [files (dir->files path)]
    (doseq [file files]
      (let [topic (path->topic (.getName file))]
        (log/info "populating new topic" topic)
        (file->stream out file topic)))))

(defn spout-dir
  [config path]
  (let [host (:host config)
        spout (producer host)]
    (dir->streams spout path)))

;; (defn kafka-host
;;   []
;;   (or
;;    (System/getenv "KAFKA_HOST")
;;    "localhost"))

;; (def default-config
;;   {:host (str (kafka-host) ":9092")
;;    :consumer
;;    {:group-id (uuid)}})

(def parse-args
  [["-k" "--kafka KAFKA" "host for kafka server"
    :default "localhost:9092"]
   ["-t" "--topic TOPIC" "kafka topic"]
   ["-o" "--output OUTPUT" "output file or dir"]
   ["-i" "--input INPUT" "input file or dir"]])

(defn -main
  [& args]
  (let [switch (first args)
        env (:options (cli/parse-opts (rest args) parse-args))
        kafka (:kafka env)
        parts (string/split kafka #":")
        host (string/join ":" (butlast parts))
        port (last parts)
        topics (if (:topic env) (string/split (:topic env) #",") [])]
    (condp = switch
      "spout"
      (doseq [path (string/split (:input env) #",")]
        (log/info "spouting" path)
        (spout-dir {:host kafka} path))

      "dump"
      (let [in (consumer {:host kafka :topics topics})
            out (io/writer (str (:output env) ".json"))]
        (consume in (fn [message] (.write out message)))
        (.close out))

      "purge"
      (let [zk (zookeeper-utils (str host ":2181"))]
        (doseq [topic topics]
          (log/info "purging" topic)
          (purge-topic! zk {:host kafka} topic))
        (log/info "closing zookeeper connection")
        (.close zk)))))
