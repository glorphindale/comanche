(ns comanche.testing
  (:require [taoensso.timbre :as timbre
             :refer (trace debug info warn error fatal spy with-log-level)]))

(info "NOOOOOO")
(info (Exception. "EXCEPTION!!!" ))
