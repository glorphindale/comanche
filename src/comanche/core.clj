(ns comanche.core
  (:import [org.jeromq ZMQ])
  (:require [taoensso.timbre :as timbre
              :refer (trace debug info warn error fatal spy with-log-level)])
  )

(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "logs.log")

(defn new-node [id]
  {:id id :state :new :location (str "tcp://127.0.0.1:900" id)})

(def nodes
  (map new-node (range 1)))

(def ctx (ZMQ/context 1))

(defn get-echo-fun [node]
  (fn []
    (try 
      (let [s (.socket ctx ZMQ/REP)
            {:keys [location id]} node]
        (.bind s location)
        (loop [in-msg (.recv s)]
          (info (str "node " id " received message"))
          (.send s in-msg)
          (recur (.recv s))))
      (catch Exception e (info (str "Node: Caught exc " e)))
      )))

(defn send-msg
  [msg location]
  (try
    (let [s (.socket ctx ZMQ/REQ)]
      (info (str "connecting to " location))
      (.connect s location)
      (.send s msg)
      (info (str "Response " (String. (.recv s))))
      (.close s))
    (catch Exception e (info (str "Send: Caught exc " e)))
    ))

#_(def echo-servers 
  (reduce into (map #(future-call (get-echo-fun %)) nodes) []))

(def first-echo (future-call (get-echo-fun (first nodes))))

#_(future-cancel first-echo)

(send-msg "wASSUP" (:location (first nodes)))

