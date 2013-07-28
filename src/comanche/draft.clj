(ns comanche.draft)

(def T 4000)

(defn change-state [cluster id state]
  (update-in cluster [id :state] state))

(defn send-msg [cluster my-id target-id msg]
  (try
    (IMPLEMENT ME)
    (catch Exception ex ())))

(defn send-msg-and-expect [cluster my-id target-id in-msg exp-msg]
  (let [response (send-msg cluster my-id target-id in-msg exp-msg)]
    (cond (= response exp-msg) :ok
          :else :failure)))

(defn ping-king [cluster my-id]
  (send-msg-and-expect cluster my-id (find-king cluster) "PING" "PONG"))

(defn send-alive [cluster my-id target-id]
  (send-msg-and-expect cluster my-id target-id "ALIVE?" "FINETHANKS"))

(defn broadcast-alive [cluster my-id]
  (for [older-node (get-older-nodes cluster my-id)]
    (send-alive cluster my-id older-node-id)))

(defn broadcast-king [cluster my-id]
  (for [younger-node (get-younger-nodes cluster my-id)]
    (send-king cluster my-id younger-node-id)))

; Meta description
(defn new-node [id]
  {:id id :state :election :location (get-location id) :pending-action nil})

(defn ping [] 
  (let [response (ping-king cluster my-id (* 4 T))]
    (if (= response :failure)
      (do
        (change-state cluster king-id :gone)
        (change-state cluster my-id :election)))))

(defn election []
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

(defn election-loop []
  (while true
    (Thread/sleep T)
    (ping)
    (election)))

(defn receive-loop [cluster my-id]
  (while true
    (let [[sender-id msg] (receive-msg)]
      (cond 
        (= msg "IMTHEKING") (change-state cluster sender-id :king) 
        (and (= msg "ALIVE?") (= my-id (count cluster))) (do
                                                   (respond with "IMTHEKING")
                                                   (change-state cluster my-id :king)
                                                   (broadcast-king cluster my-id)) 
        (= msg "ALIVE?") (do
                           (respond with "FINETHANKS")
                           (change-state cluster my-id :election))
        :else (respond with "WAT?")))))
