(ns protograph.kafka
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
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
  ;; ([host] (consumer host (uuid)))
  ;; ([host group-id] (consumer host group-id []))
  ;; ([host group-id topics] (consumer host group-id topics {}))
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

(defn consume
  [in handle-message]
  (while true
    (let [records (.poll in 1000)]
      (doseq [record records]
        (handle-message record)))))

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

(defn consumer-empty?
  [in]
  (empty? (.poll in 1000)))

(defn purge-topic!
  [zk config topic]
  (set-topic-config! zk topic {"retention.ms" "1000"})
  (loop [n 0]
    (println "looping!" n)
    (let [in (consumer (assoc config :topics [topic]))]
      (if (consumer-empty? in)
        (set-topic-config! zk topic {"retention.ms" "-1"})
        (do
          (.close in)
          (Thread/sleep 1000)
          (recur (inc n)))))))

(defn path->topic
  [path]
  "removes the suffix at the end of a path"
  (let [parts (string/split path #"\.")]
    (string/join "." (butlast parts))))

(defn path->label
  [path]
  "extracts the penultimate element out of a path"
  (let [parts (string/split path #"\.")]
    (-> parts reverse (drop 1) first)))

(defn topic->label
  [topic]
  "returns the suffix at the end of a path"
  (let [parts (string/split topic #"\.")]
    (last parts)))

(defn file->stream
  [out file topic]
  (doseq [line (line-seq (io/reader file))]
    (send-message out topic line)))

(defn dir->streams
  [out path]
  (let [files (filter #(.isFile %) (file-seq (io/file path)))]
    (doseq [file files]
      (let [topic (path->topic (.getName file))]
        (log/info "populating new topic" topic)
        (file->stream out file topic)))))

(defn spout-dir
  [config path]
  (let [host (:host config)
        spout (producer host)]
    (dir->streams spout path)))

(defn kafka-host
  (or (System/getenv "KAFKA_HOST") "localhost"))

(def default-config
  {:host (str (kafka-host) ":9092")
   :consumer
   {:group-id (uuid)}})

(defn -main
  [& args]
  (condp = (first args)
    "spout"
    (doseq [path (rest args)]
      (log/info "spouting" path)
      (spout-dir default-config path))

    "purge"
    (let [zk (zookeeper-utils (str (kafka-host) ":2181"))]
      (doseq [topic (rest args)]
        (log/info "purging" topic)
        (purge-topic zk topic))
      (log/info "closing zookeeper connection")
      (.close zk))))
