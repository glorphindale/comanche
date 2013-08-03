(ns comanche.draft
  (:require [taoensso.timbre :as timbre
              :refer (debug info)]
            [clojure.string :refer (split)]
            [clojure.edn :as edn])
  (:import [org.jeromq ZMQ]))

(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "logs.log")
(timbre/set-config! [:current-level] :info)

(def T 4000)
(def ctx (ZMQ/context 1))

; Cluster generation
(defn get-location [id]
  (let [sid (+ 9000 id)]
    (str "tcp://127.0.0.1:" sid)))

(defn new-node [id]
  {:id id :location (get-location id)})

(defn gen-new-cluster [size]
  (vec (map new-node (range 0 size))))

;
(def cluster
  (edn/read-string (slurp "cluster.conf")))

; Transmission layer
(defn send-msg
  "Msg must be in number:message format"
  ([my-id target-id msg]
   (send-msg my-id target-id msg T))
  ([my-id target-id msg timeout]
   (try
     (let [s (.socket ctx ZMQ/REQ)
           target (:location (cluster target-id))]
       (debug "Node" my-id ":" "Sending msg " msg " from " my-id " to " target-id ": " target)
       (.setReceiveTimeOut s timeout)
       (.setSendTimeOut s timeout)
       (.connect s target)
       (debug "Node" my-id ":" "connected")
       (.send s msg)
       (debug "Node" my-id ":" "sent")
       (let [response (String. (.recvStr s))]
         (debug "Node" my-id ":" "Received" response)
         response))
     (catch Exception e :failure #_(do
                          (debug "Node" my-id ":" "Exception when sending message " msg "to" target-id ":" e)
                          :failure)))))
(defn make-msg [id msg]
  (str id ":" msg))

(defn split-msg [msg]
  (let [[id text] (split msg #":")
        out-id (Integer. id)]
    [out-id text]))

; Transport layer
(defn send-msg-and-expect
  ([my-id target-id in-msg out-msg]
   (send-msg-and-expect my-id target-id in-msg out-msg T))
  ([my-id target-id in-msg out-msg timeout]
   (debug "Node" my-id ":" "send-msg-and-expect")
   (let [msg (make-msg my-id in-msg)
         exp-msg (make-msg target-id out-msg)
         response (send-msg my-id target-id msg timeout)]
     (cond (= response exp-msg) :ok
           :else :failure))))

(defn split-cluster [pivot]
  [(subvec cluster 0 pivot) (subvec cluster (inc pivot))])

(defn ping-king [my-id king-id]
  (debug "Node" my-id ":" "In ping king")
  (send-msg-and-expect my-id king-id "PING" "PONG" (* 4 T)))

(defn send-alive [my-id target-id]
  (debug "Node" my-id ":" "Send alive" my-id ":" target-id)
  (send-msg my-id target-id (make-msg my-id "ALIVE?")))

(defn send-king [my-id target-id]
  (send-msg-and-expect my-id target-id "IMTHEKING" "OK"))

(defn broadcast-alive [my-id]
  (debug "Node" my-id ":" "Broadcast alive")
  (let [older-nodes (second (split-cluster my-id))]
    (vec (map
         (fn [older-node] (future (send-alive my-id (:id older-node))))
         older-nodes))))

(defn broadcast-king [my-id]
  (debug "Node" my-id ":" "Broadcasting kingness")
  (let [younger-nodes (first (split-cluster my-id))]
    (vec (map
           (fn [younger-node] (future (send-king my-id (:id younger-node))))
           younger-nodes))))

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
       (map deref)
       (filter #(not= % :failure))
       (map split-msg)
       (filter (fn [[_ msg]] (= msg searchee)))
       (map (fn [[id _]] id))))

; Meta description
(defn ping [knowledge my-id]
  (debug "Node" my-id ":" "PING")
  (if-let [king-id (find-king! knowledge)]
    (let [response (ping-king my-id king-id)]
      (debug "Node" my-id ":" king-id "response is " response)
      (if (= response :failure)
        (king-lost! knowledge)))))

; Sending handler
(defn election-cycle [knowledge my-id]
  (debug "Node" my-id ":" "new election cycle")
  (let [broadcast-results (broadcast-alive my-id)
        finethanks (dig-broadcast broadcast-results "FINETHANKS")
        imtheking (dig-broadcast broadcast-results "IMTHEKING")]
    (debug "Node" my-id ":" "Finethanks:" finethanks "imtheking:" imtheking)
    (cond (not-empty imtheking) (do
                                  (change-state! knowledge :stable)
                                  (king-found! knowledge (first imtheking)))
          (empty? finethanks) (do
                                (broadcast-king my-id)
                                (change-state! knowledge :king))
          (not-empty finethanks) (do
                                   (Thread/sleep T))
          :else (debug "Node" my-id ":" "Election-cycle failed"))))

(defn election [knowledge my-id]
  (while (= :election (@knowledge :state))
    (election-cycle knowledge my-id)))

(defn state-loop [knowledge my-id]
  (while true
    (do
      (info "Node" my-id ":" @knowledge)
      (let [state (:state @knowledge)]
        (cond (= :election state) (election knowledge my-id)
              (= :stable state) (ping knowledge my-id)
              :else true)))
      (Thread/sleep T)))

; Receiving handler
(defn transition [knowledge my-id in-msg]
  (debug "Node" my-id ": Transition" in-msg @knowledge)
  (let [[sender-id msg] (split-msg in-msg)]
    (cond
      (= msg "PING") "PONG"
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
      (= msg "REPORT!") @knowledge
      :else "WAT?")))

(defn receive-loop [knowledge my-id]
  (let [location (get-in cluster [my-id :location])
        sock (.socket ctx ZMQ/REP)]
    (try
      (debug "Node" my-id ":" "Starting receive-loop")
      (.bind sock location)
      (.setSendTimeOut sock T)
      (loop [in-msg (.recvStr sock)]
        (debug "Node" my-id ":" "Received" in-msg)
        (let [response (transition knowledge my-id in-msg)
              out-msg (str my-id ":" response)]
          (debug "Node" my-id ":" "Sending out" out-msg)
          (.send sock out-msg))
        (recur (.recvStr sock)))
      (catch Exception e (debug "Node" my-id ": Caught exception" e)))))

(defn node-march [id]
  (let [knowledge (atom {:state :election :king nil})
        inbound (future (receive-loop knowledge id))
        outbound (future (state-loop knowledge id))]
    (info "started node" id)
    [inbound outbound]))

(defn -main [& args]
  (let [args (set args)
        ids (map #(Integer. %) args)]
    (if (or (empty? ids)
            (not (every? #(< -1 % (count cluster)) ids)))
      (println "Node ids should be between 0 and" (dec (count cluster)))
      (let [nodes (map node-march ids)
            waiting-for (first (first nodes))]
        (doall nodes)
        @waiting-for)))
  (shutdown-agents))
