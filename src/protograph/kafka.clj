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
   {;; "auto.offset.reset" "earliest"
    "enable.auto.commit" "true"
    "auto.commit.interval.ms" "1000"
    "max.poll.records" "80"
    "max.poll.interval.ms" "30000"
    "request.timeout.ms" "40000"
    "session.timeout.ms" "30000"}))

(defn uuid
  []
  (str (UUID/randomUUID)))

(defn compose-host
  [config]
  (str (get config :host "localhost") ":" (get config :port "9092")))

(defn producer
  ([] (producer {}))
  ([config]
   (log/info "kafka producer" config)
   (let [host (compose-host config)
         base {"bootstrap.servers" host}
         props (merge string-serializer base (or (:producer config) {}))]
     (new KafkaProducer props))))

(defn send-message
  [producer topic message]
  (let [record (new ProducerRecord topic (uuid) message)]
    (.send producer record)))

(defn consumer
  ([] (consumer {}))
  ([{:keys [group-id topics props] :as config}]
   (log/info "kafka consumer" config)
   (let [config (merge
                 consumer-defaults
                 {"bootstrap.servers" (compose-host config)
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
  (loop [records (.poll in Long/MAX_VALUE)]
    (when-not (.isEmpty records)
      (doseq [record records]
        (handle-message record))
      (recur (.poll in Long/MAX_VALUE)))))

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
        (log/info "reading topics" topics "from" kafka)
        (consume in (fn [message] (.write out (str (.value message) "\n"))))
        (.close out))

      "purge"
      (let [zk (zookeeper-utils (str host ":2181"))]
        (doseq [topic topics]
          (log/info "purging" topic)
          (purge-topic! zk {:host kafka} topic))
        (log/info "closing zookeeper connection")
        (.close zk)))))
