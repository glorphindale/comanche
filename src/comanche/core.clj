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

(def nodes
  (assoc
    (zipmap (range N) (map new-node (range N)))
    N (new-node N :king)))

(def ctx (ZMQ/context 1))

(def TIMEOUT 4000)

(defn transition [in-msg]
  (case in-msg
    "PING" "PONG"
    "WAT?"))

(defn find-king [node-map]
  (val (first (filter (fn [[_ v]] (= :king (:state v))) node-map))))

(defn send-ping [king-node]
  (try
    (let [s (.socket ctx ZMQ/REQ)]
      (.setReceiveTimeOut s TIMEOUT)
      (.setSendTimeOut s TIMEOUT)
      (.connect s (:location king-node))
      (.send s "PING")
      (let [msg (String. (.recvStr s))]
        (if (not= "PONG" msg)
          :failure
          :ok)))
    (catch Exception e (do
                         (info (str "PING failed: caught exc " e))
                         :failure))))

(defn handler [node-map id]
  (fn []
    (let [me (node-map id)
          {:keys [id state]} me
          port (:location me)
          s (.socket ctx ZMQ/REP)]
      (if (= state :king)
        (try
          (info (str "king node listening" ))
          (.bind s port)
          (.setSendTimeOut s TIMEOUT)
          (loop [msg (.recvStr s)]
            (info (str "node " id " received message " msg))
            (.send s (transition msg))
            (recur (.recvStr s)))
          (catch Exception e (info (str "Node : Caught exc " e))))
        (while true
          (do
            (info (str "Received " (send-ping (find-king node-map)) " when pinging king node"))
            (Thread/sleep 5000)))))))

(defn get-handlers []
  (map #(future-call (handler nodes (:id %))) (vals nodes)))

(defn -main [& args]
  (let [args (set args)
        id (Integer. (first args))]
    (if (nil? id)
      (throw "Provide node id")
      (let [h (future-call (handler nodes id))]
        (info "Waiting for future to end")
        @h))))
