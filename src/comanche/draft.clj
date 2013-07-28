(ns comanche.draft)

(def T 4000)

(defn send-msg [cluster my-id target-id msg]
  (try
    (let [s (.socket ctx ZMQ/REQ)
          whole-msg (str my-id ":" msg)]
      (-> s 
          (.setReceiveTimeOut T)
          (.setSendTimeOut T)
          (.connect (:location (cluster target-id)))
          (.send whole-msg))
      (String. (.recvStr s)))
    (catch Exception e :failure)))

(defn change-state [cluster id state]
  (update-in cluster [id :state] state))

(defn find-king [cluster]
  (filter #(= (:state % ) :king) cluster))

(defn split-cluster [cluster pivot]
  [(subvec cluster 0 pivot) (subvec cluster (inc pivot))])

(defn send-msg-and-expect [cluster my-id target-id in-msg exp-msg]
  (let [response (send-msg cluster my-id target-id in-msg exp-msg)]
    (cond (= response exp-msg) :ok
          :else :failure)))

(defn ping-king [cluster my-id]
  (send-msg-and-expect cluster my-id (find-king cluster) "PING" "PONG"))

(defn send-alive [cluster my-id target-id]
  (send-msg-and-expect cluster my-id target-id "ALIVE?" "FINETHANKS"))

(defn broadcast-alive [cluster my-id]
  (for [older-node (nth (split-cluster cluster my-id) 1)]
    (send-alive cluster my-id (:id older-node))))

(defn broadcast-king [cluster my-id]
  (for [younger-node (nth (split-cluster my-id) 0)]
    (send-king cluster my-id (:id younger-node))))

; Meta description
(defn new-node [id]
  {:id id :state :election :location (get-location id) :pending-action nil})

(defn ping [cluster my-id] 
  (let [response (ping-king cluster my-id (* 4 T))]
    (if (= response :failure)
      (do
        (change-state cluster king-id :gone)
        (change-state cluster my-id :election)))))

(defn election [cluster my-id]
  (while (= my-state :election)
    (let [broadcast-results (broadcast-alive cluster-id my-id)
          finethanks (count "FINETHANKS" in broadcast-results)
          imtheking (count "IMTHEKING" in broadcast-results)]
      (if (empty? finethanks) 
        (do
          (change-state cluster my-id :king)
          (broadcast-king cluster my-id)))
      (if (not-empty imtheking)
        (change-state cluster my-id :stable)))))

(defn election-loop [cluster my-id]
  (while true
    (Thread/sleep T)
    (ping cluster my-id)
    (election cluster my-id)))

(defn transition [cluster my-id in-msg]
  (let [[sender-id msg] in-msg]
    (cond 
      (= msg "IMTHEKING") (do
                            (change-state cluster sender-id :king)
                            "OK") 
      (and (= msg "ALIVE?") (= my-id (count cluster))) (do
                                                         (change-state cluster my-id :king)
                                                         (broadcast-king cluster my-id)  
                                                         "IMTHEKING")
      (= msg "ALIVE?") (do
                         (change-state cluster my-id :election) 
                         "FINETHANKS")
      :else "WAT?")))

(defn receive-loop [cluster my-id]
  (let [location (get-in cluster [my-id :location])
        sock (.socket ctx ZMQ/REP)]
    (try
      (.bind sock location)
      (.setSendTimeOut sock T)
      (loop [in-msg (.recvStr sock)]
        (let [response (transition in-msg)
              out-msg (str my-id ":": response)]
          (.send sock out-msg))
        (recur (.recvStr sock)))
      (catch Exception e nil)))) 
