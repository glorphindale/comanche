(ns comanche.signals
  (:require [taoensso.timbre :as timbre :refer (debug info error)]
            [clojure.edn :as edn]
            [comanche.smoke :as smoke]
            [comanche.constants :as constants]))

(defn split-cluster [cluster pivot]
  "Split cluster by pivot indes, return [younger nodes, older nodes] vector"
  [(subvec cluster 0 pivot) (subvec cluster (inc pivot))])

(defn ping-king [my-id king-node]
  (debug "Node" my-id ":" "In ping king")
  (smoke/send-msg-and-expect my-id king-node "PING" "PONG" (* 4 constants/TIMEOUT)))

(defn send-alive [my-id target-node]
  (debug "Node" my-id ":" "Send alive" my-id ":" (:id target-node))
  (smoke/send-msg my-id target-node (smoke/make-msg my-id "ALIVE?")))

(defn send-king [my-id target-node]
  (smoke/send-msg-and-expect my-id target-node "IMTHEKING" "OK"))

(defn broadcast-msg
   "Send msg to nodes in a set of futures and return them"
  ([my-id nodes msg]
   (broadcast-msg my-id nodes msg constants/TIMEOUT))
  ([my-id nodes msg timeout]
   (vec (map
          (fn [node] (future (smoke/send-msg my-id node (smoke/make-msg my-id msg) timeout)))
          nodes))))

(defn broadcast-alive [cluster my-id]
  (debug "Node" my-id ":" "Broadcast alive")
  (let [older-nodes (second (split-cluster cluster my-id))]
    (broadcast-msg my-id older-nodes "ALIVE?")))

(defn broadcast-king [cluster my-id]
  (debug "Node" my-id ":" "Broadcasting kingness")
  (let [[younger older] (split-cluster cluster my-id)
        not-mes (into younger older)]
    (broadcast-msg my-id not-mes "IMTHEKING")))

(defn dig-broadcast [received searchee]
  "Take result of broadcast and return ids of nodes, that replied with searchee"
  (->> received
       (map deref)
       (filter #(not= % :failure))
       (map smoke/split-msg)
       (filter (fn [[_ msg]] (= msg searchee)))
       (map (fn [[id _]] id))))

; Helper functions
(defn broadcast-exit [my-id nodes]
  (broadcast-msg my-id nodes "EXIT!"))

(defn broadcast-report [my-id nodes]
  (broadcast-msg my-id nodes "REPORT!"))

(defn dig-report [broadcast]
  "Take result of broadcast-report and return a map of results"
  (->> broadcast
       (map deref)
       (map smoke/split-msg)
       (map (fn [[id resp]] [id (edn/read-string resp)]))
       (into {})))

(defn cluster-status [cluster]
  "Take a cluster and return what different nodes think of the cluster status"
  (let [raw (broadcast-report -1 cluster)]
    (Thread/sleep constants/TIMEOUT)
    (let [output (dig-report raw)]
      (frequencies (vals output)))))
