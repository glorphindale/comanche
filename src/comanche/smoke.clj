(ns comanche.smoke
  (:require [taoensso.timbre :as timbre
             :refer (debug info error)]
            [clojure.string :as string]
            [comanche.constants :as constants])
  (:import [org.jeromq ZMQ]))

(def ctx (ZMQ/context 1))

(defn stop-zmq []
  (.term ctx))

(defn make-msg [id msg]
  (str id ":" msg))

(defn split-msg [msg]
  (try
      (let [[id text] (string/split msg #":" 2)
          out-id (Integer. id)]
      [out-id text])
    (catch Exception e "-1::failure")))

(defn send-msg
  "Connect to a specified node, send message and read a response.
  Msg must be in a number:message format"
  ([my-id target-node msg]
   (send-msg my-id target-node msg constants/TIMEOUT))
  ([my-id target-node msg timeout]
   (let [target-id (:id target-node)
         target (:location target-node)]
     (debug "Node" my-id ":" "Sending msg " msg " from " my-id " to " target-id ": " target)
     (try
       (with-open [sock (.socket ctx ZMQ/REQ)]
         (doto sock
             (.setReceiveTimeOut timeout)
             (.setSendTimeOut timeout)
             (.setLinger timeout)
             (.connect target)
             (.send msg))
         (if-let [raw (.recvStr sock)]
           (String. raw)
           (do
             (error "Node" my-id ":" "Empty socket when sending " msg "to" target-id)
             (make-msg target-id :failure))))
       (catch Exception e (do
                            (error "Node" my-id ":" "Exception when sending message " msg "to" target-id ":" e)
                            (make-msg target-id :failure)))))))
(defn send-msg-and-expect
  ([my-id target-node in-msg out-msg]
   (send-msg-and-expect my-id target-node in-msg out-msg constants/TIMEOUT))
  ([my-id target-node in-msg out-msg timeout]
   (debug "Node" my-id ":" "send-msg-and-expect")
   (let [msg (make-msg my-id in-msg)
         exp-msg (make-msg (:id target-node) out-msg)
         response (send-msg my-id target-node msg timeout)]
     (cond (= response exp-msg) :ok
           :else (do
                   (info "Node" my-id ": Received" response "when waiting for" exp-msg)
                   :failure)))))

(defn receive-func [my-node knowledge transition-func exit-func]
  (let [{:keys [id location]} my-node]
    (try
      (debug "Node" id ":" "Starting receive-loop")
      (with-open [sock (.socket ctx ZMQ/REP)]
        (doto sock
          (.bind location)
          (.setSendTimeOut constants/TIMEOUT)
          (.setLinger constants/TIMEOUT))
        (loop [in-msg (.recvStr sock)]
          (debug "Node" id ":" "Received" in-msg)
          (let [response (transition-func knowledge id in-msg)
                out-msg (str id ":" response)]
            (debug "Node" id ":" "Sending out" out-msg)
            (.send sock out-msg))
          (if (not (exit-func knowledge))
            (recur (.recvStr sock)))))
      (catch Exception e (debug "Node" id ": Caught exception" e)))))

