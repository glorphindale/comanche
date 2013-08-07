(ns comanche.smoke
  (:require [taoensso.timbre :as timbre
             :refer (debug info error)]
            [clojure.string :as string])
  (:import [org.jeromq ZMQ]))

(def T 4000)

(def ctx (ZMQ/context 1))

(defn stop-zmq []
  (.term ctx))

; Transmission layer
(defn send-msg
  "Connect to a specified node, send message and read a response.
  Msg must be in a number:message format"
  ([cluster my-id target-id msg]
   (send-msg cluster my-id target-id msg T))
  ([cluster my-id target-id msg timeout]
   (let [target (:location (cluster target-id))]
     (debug "Node" my-id ":" "Sending msg " msg " from " my-id " to " target-id ": " target)
     (try
       (with-open [sock (.socket ctx ZMQ/REQ)]
         (.setReceiveTimeOut sock timeout)
         (.setSendTimeOut sock timeout)
         (.setLinger sock timeout)
         (.connect sock target)
         (.send sock msg)
         (if-let [raw (.recvStr sock)]
           (String. raw)
           (do
             (error "Node" my-id ":" "Empty socket when sending " msg "to" target-id)
             :failure)))
       (catch Exception e (do
                            (error "Node" my-id ":" "Exception when sending message " msg "to" target-id ":" e)
                            :failure))))))
; Transport layer
(defn make-msg [id msg]
  (str id ":" msg))

(defn split-msg [msg]
  (let [[id text] (string/split msg #":")
        out-id (Integer. id)]
    [out-id text]))

(defn send-msg-and-expect
  ([cluster my-id target-id in-msg out-msg]
   (send-msg-and-expect cluster my-id target-id in-msg out-msg T))
  ([cluster my-id target-id in-msg out-msg timeout]
   (debug "Node" my-id ":" "send-msg-and-expect")
   (let [msg (make-msg my-id in-msg)
         exp-msg (make-msg target-id out-msg)
         response (send-msg cluster my-id target-id msg timeout)]
     (cond (= response exp-msg) :ok
           :else (do
                   (info "Node" my-id ": Received" response "when waiting for" exp-msg)
                   :failure)))))

(defn split-cluster [cluster pivot]
  "Split cluster by pivot indes, return [younger nodes, older nodes] vector"
  [(subvec cluster 0 pivot) (subvec cluster (inc pivot))])

(defn ping-king [cluster my-id king-id]
  (debug "Node" my-id ":" "In ping king")
  (send-msg-and-expect cluster my-id king-id "PING" "PONG" (* 4 T)))

(defn send-alive [cluster my-id target-id]
  (debug "Node" my-id ":" "Send alive" my-id ":" target-id)
  (send-msg cluster my-id target-id (make-msg my-id "ALIVE?")))

(defn send-king [cluster my-id target-id]
  (send-msg-and-expect cluster my-id target-id "IMTHEKING" "OK"))

(defn broadcast-msg [cluster my-id nodes msg]
  "Send msg to nodes in a set of futures and return them"
  (vec (map
         (fn [node] (future (send-msg cluster my-id (:id node) (make-msg my-id msg))))
         nodes)))

(defn broadcast-alive [cluster my-id]
  (debug "Node" my-id ":" "Broadcast alive")
  (let [older-nodes (second (split-cluster cluster my-id))]
    (broadcast-msg cluster my-id older-nodes "ALIVE?")))

(defn broadcast-king [cluster my-id]
  (debug "Node" my-id ":" "Broadcasting kingness")
  (let [younger-nodes (first (split-cluster cluster my-id))]
    (broadcast-msg cluster my-id younger-nodes "IMTHEKING")))

(defn dig-broadcast [received searchee]
  "Take result of broadcast and return ids of nodes, that replied with searchee"
  (->> received
       (map deref)
       (filter #(not= % :failure))
       (map split-msg)
       (filter (fn [[_ msg]] (= msg searchee)))
       (map (fn [[id _]] id))))

(defn receive-func [cluster my-id knowledge transition-func exit-func]
  (try
    (debug "Node" my-id ":" "Starting receive-loop")
    (with-open [sock (.socket ctx ZMQ/REP)]
      (.bind sock (get-in cluster [my-id :location]))
      (.setSendTimeOut sock T)
      (.setLinger sock T)
      (loop [in-msg (.recvStr sock)]
        (debug "Node" my-id ":" "Received" in-msg)
        (let [response (transition-func knowledge my-id in-msg)
              out-msg (str my-id ":" response)]
          (debug "Node" my-id ":" "Sending out" out-msg)
          (.send sock out-msg))
        (if (not (exit-func knowledge))
          (recur (.recvStr sock)))))
    (catch Exception e (debug "Node" my-id ": Caught exception" e))))

