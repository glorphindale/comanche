(ns comanche.draft
  (:require [taoensso.timbre :as timbre
              :refer (trace debug info warn error fatal spy with-log-level)]
            [clojure.string :refer (split)])
  (:import [org.jeromq ZMQ]))

(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "logs.log")

(def T 4000)
(def N 3)
(def ctx (ZMQ/context 1))

(defn get-location [id]
  (str "tcp://127.0.0.1:900" id))

(defn new-node [id]
  {:id id :location (get-location id)})

(def cluster
  (vec (map new-node (range 0 N))))

(defn send-msg [my-id target-id msg]
  (info "Entering send-msg")
  (try
    (let [s (.socket ctx ZMQ/REQ)
          target (:location (cluster target-id))
          whole-msg (str my-id ":" msg)]
      (info "Sending msg " whole-msg " from " my-id " to " target-id ": " target)
      (.setReceiveTimeOut s T)
      (.setSendTimeOut s T)
      (.connect s target)
      (info "connected")
      (.send s whole-msg)
      (info "sent")
      (let [response (String. (.recvStr s))]
        (info "Received" response)))
    (catch Exception e (do 
                         (info "Exception when sending message:" e)
                         :failure))))

(defn send-msg-and-expect [my-id target-id in-msg exp-msg]
  (let [response (send-msg my-id target-id in-msg exp-msg)]
    (cond (= response exp-msg) :ok
          :else :failure)))

(defn split-cluster [pivot]
  [(subvec cluster 0 pivot) (subvec cluster (inc pivot))])

; TODO should use 4*T timeout
(defn ping-king [my-id king-id]
  (info "In ping king")
  (send-msg-and-expect my-id king-id "PING" "PONG"))

(defn send-alive [my-id target-id]
  (info "Send alive" my-id ":" target-id)
  (send-msg my-id target-id "ALIVE?"))

(defn send-king [my-id target-id]
  (send-msg-and-expect my-id target-id "IMTHEKING" "OK"))

(defn broadcast-alive [my-id]
  (info "Broadcast alive")
  (let [[_ older-nodes] (split-cluster my-id)]
    (vec (map
         (fn [older-node] (send-alive my-id (:id older-node)))
         older-nodes))))

(defn broadcast-king [my-id]
  (for [[younger-node _] (split-cluster my-id)]
    (send-king my-id (:id younger-node))))

; State management
(defn find-king [knowledge]
  (:king @knowledge))

(defn change-state! [knowledge state]
  (swap! knowledge assoc :state state))

(defn king-found! [knowledge king-id]
  (swap! knowledge assoc :king king-id))

(defn king-lost! [knowledge]
  (swap! knowledge assoc :king nil))

; Meta description
(defn ping [knowledge my-id] 
  (info "PING")
  (let [king-id (find-king knowledge)]
    (info "King is " king-id)
    (if (and king-id (not= king-id my-id))
      (let [response (ping-king my-id king-id)]
        (if (= response :failure)
          (king-lost! knowledge))))))

(defn election-cycle [knowledge my-id]
  (info "new election cycle")
  (let [broadcast-results (broadcast-alive my-id)
        finethanks (filter #(= % "FINETHANKS") broadcast-results) ; I contain node ids, 
        imtheking (filter #(= % "IMTHEKING") broadcast-results) ; I contain node ids, 
        king-id 99]
    (info "Finethanks:" finethanks "imtheking:" imtheking)
    (cond (empty? finethanks) (do
                                #_(broadcast-king my-id)  
                                (change-state! knowledge :king))  
          (not-empty imtheking) (-> cluster
                                  (change-state! knowledge :stable)
                                  (king-found! knowledge king-id)) ; Determine king-id
          :else cluster)))

(defn election [knowledge my-id]
  (while (= :election (@knowledge :state))
    (election-cycle knowledge my-id)))

(defn election-loop [knowledge my-id]
  (info "Starting election-loop")
  (while true
    (do
      (Thread/sleep T)
      (info "Ping/election" @knowledge)
      (election knowledge my-id)   
      (ping knowledge my-id))))

; Receiving handler
(defn transition [knowledge my-id in-msg]
  (info "Transition" in-msg)
  (let [[sender-id msg] (split in-msg ":")]
    (cond 
      (= msg "IMTHEKING") (do
                            (king-found! knowledge sender-id)
                            "OK") 
      (and (= msg "ALIVE?")
           (= my-id (count cluster))) (do
                                        (change-state! knowledge :king)
                                        #_(broadcast-king my-id) ; WHERE SHOULD I GO? 
                                        "IMTHEKING")
      (= msg "ALIVE?") (do
                         (change-state! knowledge :election) 
                         "FINETHANKS")
      :else "WAT?")))

(defn receive-loop [knowledge my-id]
  (let [location (get-in cluster [my-id :location])
        sock (.socket ctx ZMQ/REP)]
    (try
      (info "Starting receive-loop")
      (.bind sock location)
      (.setSendTimeOut sock T)
      (loop [in-msg (.recvStr sock)]
        (info "Received " in-msg)
        (let [response (transition knowledge my-id in-msg)
              out-msg (str my-id ":" response)]
          (info "Sending" out-msg)
          (.send sock out-msg))
        (info "Recuring")
        (recur (.recvStr sock)))
      (catch Exception e nil)))) 

(defn -main [& args]
  (let [args (set args)
        id (Integer. (first args))]
    (if (or (nil? id) (< id 0) (>= id (count cluster)))
      (println "Provide node id between 1 and" (count cluster))
      (let [knowledge (atom {:state :election :king nil})
            inbound (future (receive-loop knowledge id))
            outbound (future (election-loop knowledge id))]
        (println cluster)
        @inbound))))
