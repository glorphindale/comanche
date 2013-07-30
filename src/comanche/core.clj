(ns comanche.core
  (:import [org.jeromq ZMQ])
  (:require [taoensso.timbre :as timbre
              :refer (trace debug info warn error fatal spy with-log-level)])
  )

(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "logs.log")

(defn new-node
  ([id]
    (new-node id :election))
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

(defn node-alive? [node]
  (try
    (if-let [response (send-msg-and-expect node "ALIVE?" "FINETHANKS")]
      response
      :failure)
    (catch Exception e :failure)))

(defn get-cluster-state [cluster]
  "Return map of cluster status {1 :failure 2 :ok}"
  (into {} (for [[k v] cluster] [k (node-alive? v)])))

(defn update-cluster-state [cluster state]
  (reduce (fn [m [k v]] (assoc-in m [k :state] v)) cluster state))

(defn mark-king-gone [cluster]
  (info "Marking king as gone")
  (when-let [king (find-king cluster)]
    (assoc-in cluster [(:id king) :state] :gone)))

(defn king-ping [cluster]
  (if-let [king (find-king @cluster)]
    (try
      (let [response (send-msg-and-expect (find-king @cluster) "PING" "PONG")]
        (info "Pinging king, received " response )
        (if (= response :failure)
          (swap! cluster (mark-king-gone @cluster))))
      (catch Exception e (swap! cluster (mark-king-gone @cluster))))
    (do
      (swap! (mark-king-gone @cluster))
      #_(swap! cluster (update-cluster-state @cluster (get-cluster-state @cluster))))))

; Handlers 
(defn listening-handler [cluster]
  (let [{:keys [id location state]} (:me @cluster)
        s (.socket ctx ZMQ/REP)]
    (try
      (info (str "node " id " listening"))
      (.bind s location)
      (.setSendTimeOut s NETWORK-TIMEOUT)
      (loop [msg (.recvStr s)]
        (info (str "node " id " received message " msg))
        (.send s (transition msg))
        (if (= (transition msg) "IMTHEKING")
          (swap! cluster (fn [c] (assoc-in c [3 :state] :king))))
        (recur (.recvStr s)))
      (catch Exception e (info (str "Node " id ": Caught exc " e))))))

(defn outbound-handler [cluster]
  (info (str "Launching outbound handler"))
  (while true
    (let [me (:me @cluster)
          id (:id me)
          my-state (:state me)]
      (case my-state
        :king (info (str "Node " id " is a king, sleeping"))
        :election (info "Election mode, not implemented yet")
        :wait-for-king (info "Waiting for king to respond")
        :ok (king-ping cluster)))
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
