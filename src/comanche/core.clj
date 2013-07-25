(ns comanche.core
  (:import [org.jeromq ZMQ])
  (:require [taoensso.timbre :as timbre
              :refer (trace debug info warn error fatal spy with-log-level)])
  )

(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "logs.log")

(defn new-node
  ([id]
    (new-node id :new))
  ([id state]
   {:id id :state state :location (str "tcp://127.0.0.1:900" id)}))

(def N 3)
(def NODE-TIMEOUT 5000)
(def NETWORK-TIMEOUT 4000)

(def nodes
  (assoc
    (zipmap (range N) (map new-node (range N)))
    N (new-node N :king)))

(def ctx (ZMQ/context 1))


(defn transition [in-msg]
  (case in-msg
    "PING" "PONG"
    "ALIVE?" "FINETHANKS"
    "WAT?"))

(defn find-king [cluster]
  (val (first (filter (fn [[_ v]] (= :king (:state v))) (:cluster cluster)))))

(defn split-cluster [nodes me-id]
  {:me (nodes me-id)
   :cluster (select-keys nodes
                         (disj (set (keys nodes)) me-id))})

(defn send-msg-and-expect [node in-msg expected-msg]
  (try
    (info (str "send-msg-and-expect called to node " node))
    (let [s (.socket ctx ZMQ/REQ)]
      (.setReceiveTimeOut s NETWORK-TIMEOUT)
      (.setSendTimeOut s NETWORK-TIMEOUT)
      (.connect s (:location node))
      (info "connected")
      (.send s in-msg)
      (info "sent")
      (let [out-msg (String. (.recvStr s))]
        (info "received")
        (if (not= expected-msg out-msg)
          :failure
          :ok)))
    (catch Exception e (do
                         (info (str in-msg " failed: caught exc " e))
                         :failure))))

(defn listening-handler [cluster]
  (let [{:keys [id location]} (:me @cluster)
        s (.socket ctx ZMQ/REP)]
    (try
      (info (str "node " id " listening"))
      (.bind s location)
      (.setSendTimeOut s NETWORK-TIMEOUT)
      (loop [msg (.recvStr s)]
        (info (str "node " id " received message " msg))
        (.send s (transition msg))
        (recur (.recvStr s)))
      (catch Exception e (info (str "Node " id ": Caught exc " e))))))

(defn outbound-handler [cluster]
  (info (str "Launching outbound handler"))
  (while true
    (let [me (:me @cluster)
          king (find-king @cluster)
          id (:id me)
          state (:state me)]
      (if (= state :king)
        (info (str "Node " id " is a king, sleeping"))
        (info "received" (send-msg-and-expect king "PING" "PONG"))))
    (Thread/sleep NODE-TIMEOUT)
    (info "Starting new while"))) 

(defn -main [& args]
  (let [args (set args)
        id (Integer. (first args))]
    (if (nil? id)
      (throw "Provide node id")
      (let [parted-nodes (split-cluster nodes id)
            cluster (atom parted-nodes)
            inbound (future (listening-handler cluster))
            outbound (future (outbound-handler cluster))]
        (info "Waiting for futures to end")
        @inbound))))
