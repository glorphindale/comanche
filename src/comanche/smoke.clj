(ns comanche.smoke
  (:require [taoensso.timbre :as timbre
             :refer (debug info error)]
            [clojure.string :as string]
            [clojure.edn :as edn])
  (:import [org.jeromq ZMQ]))

(def T 4000)

(def ctx (ZMQ/context 1))

(defn stop-zmq []
  (.term ctx))

; Transmission layer
(defn make-msg [id msg]
  (str id ":" msg))

(defn split-msg [msg]
  (let [[id text] (string/split msg #":" 2)
        out-id (Integer. id)]
    [out-id text]))

(defn send-msg
  "Connect to a specified node, send message and read a response.
  Msg must be in a number:message format"
  ([my-id target-node msg]
   (send-msg my-id target-node msg T))
  ([my-id target-node msg timeout]
   (let [target-id (:id target-node)
         target (:location target-node)]
     (debug "Node" my-id ":" "Sending msg " msg " from " my-id " to " target-id ": " target)
     (try
       (with-open [sock (.socket ctx ZMQ/REQ)]
         (doto sock
             (.setReceiveTimeOut timeout)
             (.setSendTimeOut timeout)
             (.setLinger timeout)
             (.connect target)
             (.send msg))
         (if-let [raw (.recvStr sock)]
           (String. raw)
           (do
             (error "Node" my-id ":" "Empty socket when sending " msg "to" target-id)
             (make-msg target-id :failure))))
       (catch Exception e (do
                            (error "Node" my-id ":" "Exception when sending message " msg "to" target-id ":" e)
                            (make-msg target-id :failure)))))))
; Transport layer
(defn send-msg-and-expect
  ([my-id target-node in-msg out-msg]
   (send-msg-and-expect my-id target-node in-msg out-msg T))
  ([my-id target-node in-msg out-msg timeout]
   (debug "Node" my-id ":" "send-msg-and-expect")
   (let [msg (make-msg my-id in-msg)
         exp-msg (make-msg (:id target-node) out-msg)
         response (send-msg my-id target-node msg timeout)]
     (cond (= response exp-msg) :ok
           :else (do
                   (info "Node" my-id ": Received" response "when waiting for" exp-msg)
                   :failure)))))

(defn split-cluster [cluster pivot]
  "Split cluster by pivot indes, return [younger nodes, older nodes] vector"
  [(subvec cluster 0 pivot) (subvec cluster (inc pivot))])

(defn ping-king [my-id king-node]
  (debug "Node" my-id ":" "In ping king")
  (send-msg-and-expect my-id king-node "PING" "PONG" (* 4 T)))

(defn send-alive [my-id target-node]
  (debug "Node" my-id ":" "Send alive" my-id ":" (:id target-node))
  (send-msg my-id target-node (make-msg my-id "ALIVE?")))

(defn send-king [my-id target-node]
  (send-msg-and-expect my-id target-node "IMTHEKING" "OK"))

(defn broadcast-msg
   "Send msg to nodes in a set of futures and return them"
  ([my-id nodes msg]
   (broadcast-msg my-id nodes msg T))
  ([my-id nodes msg timeout]
   (vec (map
          (fn [node] (future (send-msg my-id node (make-msg my-id msg) timeout)))
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
       (map split-msg)
       (filter (fn [[_ msg]] (= msg searchee)))
       (map (fn [[id _]] id))))

(defn receive-func [my-node knowledge transition-func exit-func]
  (let [{:keys [id location]} my-node]
    (try
      (debug "Node" id ":" "Starting receive-loop")
      (with-open [sock (.socket ctx ZMQ/REP)]
        (doto sock
          (.bind location)
          (.setSendTimeOut T)
          (.setLinger T))
        (loop [in-msg (.recvStr sock)]
          (debug "Node" id ":" "Received" in-msg)
          (let [response (transition-func knowledge id in-msg)
                out-msg (str id ":" response)]
            (debug "Node" id ":" "Sending out" out-msg)
            (.send sock out-msg))
          (if (not (exit-func knowledge))
            (recur (.recvStr sock)))))
      (catch Exception e (debug "Node" id ": Caught exception" e)))))

; Helper functions
(defn broadcast-exit [my-id nodes]
  (broadcast-msg my-id nodes "EXIT!"))

(defn broadcast-report [my-id nodes]
  (broadcast-msg my-id nodes "REPORT!"))

(defn dig-report [broadcast]
  "Take result of broadcast-report and return a map of results"
  (->> broadcast
       (map deref)
       (map split-msg)
       (map (fn [[id resp]] [id (edn/read-string resp)]))
       (into {})))

(defn cluster-status [cluster]
  "Take a cluster and return what different nodes think of the cluster status"
  (let [raw (broadcast-report -1 cluster)]
    (Thread/sleep T)
    (let [output (dig-report raw)]
      (frequencies (vals output)))))
