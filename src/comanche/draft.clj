(ns comanche.draft
  (:require [taoensso.timbre :as timbre
              :refer (trace debug info warn error fatal spy with-log-level)] ; TODO drop unneeded
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
          target (:location (cluster target-id))]
      #_(info "Sending msg " msg " from " my-id " to " target-id ": " target)
      (.setReceiveTimeOut s T)
      (.setSendTimeOut s T)
      (.connect s target)
      #_(info "connected")
      (.send s msg)
      #_(info "sent")
      (let [response (String. (.recvStr s))]
        #_(info "Received" response)
        response))
    (catch Exception e (do
                         (info "Exception when sending message:" e)
                         :failure))))
(defn make-msg [id msg]
  (str id ":" msg))

(defn send-msg-and-expect [my-id target-id in-msg out-msg]
  #_(info "send-msg-and-expect")
  (let [msg (make-msg my-id in-msg)
        exp-msg (make-msg target-id out-msg)
        response (send-msg my-id target-id msg)]
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
  (send-msg my-id target-id (make-msg my-id "ALIVE?")))

(defn send-king [my-id target-id]
  (send-msg-and-expect my-id target-id "IMTHEKING" "OK"))

(defn broadcast-alive [my-id]
  (info "Broadcast alive")
  (let [older-nodes (second (split-cluster my-id))]
    (vec (map
         (fn [older-node] (send-alive my-id (:id older-node)))
         older-nodes))))

(defn broadcast-king [my-id]
  (info "Broadcasting kingness")
  (future 
    (let [younger-nodes (first (split-cluster my-id))]
      (vec (map
             (fn [younger-node] (send-king my-id (:id younger-node)))
             younger-nodes)))))

; State management
(defn find-king! [knowledge]
  (:king @knowledge))

(defn change-state! [knowledge state]
  (swap! knowledge assoc :state state))

(defn king-found! [knowledge king-id]
  (swap! knowledge assoc :king king-id))

(defn king-lost! [knowledge]
  (swap! knowledge assoc :king nil)
  (change-state! knowledge :election))

(defn dig-broadcast [received searchee]
  "Take result of broadcast and return ids of nodes, that replied with searchee"
  (->> received
      (filter #(not= % :failure))
      (map #(split % #":"))
      (filter (fn [[_ msg]] (= msg searchee)))
      (map (fn [[id _]] (Integer. id)))))

; Meta description
(defn ping [knowledge my-id]
  (info "PING")
  (if-let [king-id (find-king! knowledge)]
    (let [response (ping-king my-id king-id)]
      (info king-id "response is " response)  
      (if (= response :failure)
        (king-lost! knowledge)))))

; Sending handler
(defn election-cycle [knowledge my-id]
  (info "new election cycle")
  (let [broadcast-results (broadcast-alive my-id)
        finethanks (dig-broadcast broadcast-results "FINETHANKS")
        imtheking (dig-broadcast broadcast-results "IMTHEKING")]
    (info "Finethanks:" finethanks (empty? finethanks) "imtheking:" imtheking (first imtheking))
    (cond (not-empty imtheking) (do
                                  (change-state! knowledge :stable)
                                  (king-found! knowledge (first imtheking))
                                  (info @knowledge))
          (empty? finethanks) (do
                                (broadcast-king my-id)
                                (change-state! knowledge :king)
                                (info "Should become a king " @knowledge))
          (not-empty finethanks) (do
                                   (Thread/sleep T)) ; TODO Is this approach correct?
          :else (info "Election-cycle failed"))))

(defn election [knowledge my-id]
  (while (= :election (@knowledge :state))
    (do 
      (info "election with" @knowledge)
      (election-cycle knowledge my-id))))

(defn state-loop [knowledge my-id]
  (info "Starting election-loop")
  (while true
    (do
      (let [state (:state @knowledge)]
        (cond (= :election state) (election knowledge my-id)
              (= :stable state) (ping knowledge my-id)
              :else (info "King should sleep for a while"))))
      (Thread/sleep T)))

; Receiving handler
(defn transition [knowledge my-id in-msg]
  (info "Transition" in-msg)
  (let [[sender-id msg] (split in-msg #":")]
    (cond
      (= msg "PING") "PONG" ; Do we need to check for kingness?
      (= msg "IMTHEKING") (do
                            (king-found! knowledge sender-id)
                            (change-state! knowledge :stable)
                            "OK")
      (and (= msg "ALIVE?")
           (= :king (:state @knowledge))) "IMTHEKING"
      (and (= msg "ALIVE?")
           (= my-id (dec (count cluster)))) (do
                                        (change-state! knowledge :king)
                                        (broadcast-king my-id)
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
          (info "Sending out" out-msg)
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
            outbound (future (state-loop knowledge id))]
        (println cluster)
        @inbound))))
