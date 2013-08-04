(ns comanche.draft
  (:require [taoensso.timbre :as timbre
             :refer (debug info error)]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [comanche.smoke :as smoke]))

(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "logs.log")
(timbre/set-config! [:current-level] :info)

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

; State management
(defn who-king? [knowledge]
  (:king @knowledge))

(defn me-king? [knowledge]
  (= :king (:state @knowledge)))

(defn no-king? [knowledge]
  (= :election (:state @knowledge)))

(defn stability? [knowledge]
  (= :stable (:state @knowledge)))

(defn exit? [knowledge]
  (= :exit (:state @knowledge)))

(defn exit! [knowledge]
  (reset! knowledge {:king nil :state :exit}))

(defn king-me! [knowledge]
  (reset! knowledge {:king nil :state :king}))

(defn king-found! [knowledge king-id]
  (reset! knowledge {:king king-id :state :stable}))

(defn king-lost! [knowledge]
  (reset! knowledge {:king nil :state :election}))

; Sending handler
(defn ping [knowledge my-id]
  (debug "Node" my-id ":" "PING")
  (if-let [king-id (who-king? knowledge)]
    (let [response (smoke/ping-king cluster my-id king-id)]
      (debug "Node" my-id ":" king-id "response is " response)
      (if (= response :failure)
        (king-lost! knowledge)))))

(defn election [knowledge my-id]
  (while (no-king? knowledge)
    (let [broadcast-results (smoke/broadcast-alive cluster my-id)
          finethanks (smoke/dig-broadcast broadcast-results "FINETHANKS")
          imtheking (smoke/dig-broadcast broadcast-results "IMTHEKING")]
      (debug "Node" my-id ":" "Finethanks:" finethanks "imtheking:" imtheking)
      (cond (not-empty imtheking) (king-found! knowledge (first imtheking))
            (empty? finethanks) (do
                                  (smoke/broadcast-king cluster my-id)
                                  (king-me! knowledge))
            (not-empty finethanks) (do
                                     (Thread/sleep smoke/T))
            :else (debug "Node" my-id ":" "Election-cycle failed")))))

(defn state-loop [knowledge my-id]
  (while (not (exit? knowledge))
    (do
      (info "Node" my-id ":" @knowledge)
      (cond (no-king? knowledge) (election knowledge my-id)
            (stability? knowledge) (ping knowledge my-id)
            :else true))
      (Thread/sleep smoke/T)))

; Receiving handler
(defn transition [knowledge my-id in-msg]
  (debug "Node" my-id ": Transition" in-msg @knowledge)
  (let [[sender-id msg] (smoke/split-msg in-msg)]
    (cond
      (= msg "PING") "PONG"
      (= msg "IMTHEKING") (do
                            (king-found! knowledge sender-id)
                            "OK")
      (and (= msg "ALIVE?")
           (me-king? knowledge)) "IMTHEKING"
      (and (= msg "ALIVE?")
           (= my-id (dec (count cluster)))) (do
                                        (king-me! knowledge)
                                        (smoke/broadcast-king cluster my-id)
                                        "IMTHEKING")
      (= msg "ALIVE?") (do
                         (king-lost! knowledge)
                         "FINETHANKS")
      (= msg "REPORT!") @knowledge
      (= msg "EXIT!") (exit! knowledge)
      :else "WAT?")))

(defn receive-loop [knowledge my-id]
  (smoke/receive-func cluster my-id knowledge transition exit?))

; Glue them all
(defn launch-node [id]
  (let [knowledge (atom {:state :election :king nil})
        inbound (future (receive-loop knowledge id))
        outbound (future (state-loop knowledge id))]
    (info "Started node" id)
    [knowledge inbound outbound]))

(defn get-ids [args]
  "Return a seq of node IDs to start"
  (cond (empty? args) nil
        (some #{"-f"} args) (range (count cluster))
        (and (= (count args) 1) (re-find #"-" (first args)))
          (let [[start stop] (map #(Integer. %) (string/split (first args) #"-"))]
            (if (< start stop)
              (range start (inc stop))
              nil))
        (every? #(< -1 (Integer. %) (count cluster)) args) (map #(Integer. %) args)
        :else nil))

(defn -main [& args]
  (let [args (set args)]
    (if-let [ids (get-ids args)]
      (do
        (info "Starting nodes" ids)
        (let [nodes (map launch-node ids)
              waiting-for (second (first nodes))]
          (doall nodes)
          @waiting-for)) ; TODO wait for all the nodes
      (info "Usage: run with '-f' for full cluster emulation, with 'id1 id2 ... idk' for specific set of nodes, or with 'id1-idN' for a range of nodes. Ids should be between 0 and" (dec (count cluster)) ".")))
  (shutdown-agents))
