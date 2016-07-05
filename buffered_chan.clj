(ns zph.buffered-chan
  (:require [clojure.core.async :as a]
            [clojure.core.async.impl.protocols :as p]
            [schema.core :as s]))

;; Exercise in creating BufferedChans that behave more like golang chans.

(def ^:dynamic *default-length* 10000)
(defprotocol IBufferedChan
  "A pipe contains a buffer and initialized chan.
  Functions appropriate for checking channel capacity and usage are implemented."
  (status        [bc] "map of the current state")
  (capacity      [bc] "buffer total capacity")
  (len           [bc] "queued items in channel")
  (available     [bc] "slots available for queueing (size - len)")
  (pct-available [bc] "% of (size - used) / size"))

(s/defrecord BufferedChan [name   :- s/Str
                           chan   :- s/Any
                           buffer :- s/Any
                           size   :- s/Int]
  p/ReadPort
  (take! [_ handler]
         (p/take! chan handler))

  p/WritePort
  (put! [_ msg handler]
        (p/put! chan msg handler))

  p/Channel
  (close! [_]
          (p/close! chan))

  IBufferedChan
  (status        [this] {:capacity (capacity this)
                         :len (len this)
                         :available (available this)
                         :pct_available (pct-available this)})
  (capacity      [this] size)
  (len           [this] (count buffer))
  (available     [this] (- size (len this)))
  (pct-available [this] (int (Math/floor
                              (* 100.0
                                 (/ (available this)
                                    size))))))

(defn uuid [] (str (java.util.UUID/randomUUID)))

(defn buffered-chan
  ([]                 (buffered-chan *default-length* nil   nil (uuid)))
  ([n]                (buffered-chan n   nil   nil (uuid)))
  ([n xform]          (buffered-chan n   xform nil (uuid)))
  ([n xform exh]      (buffered-chan n   xform exh (uuid)))
  ([n xform exh name] (buffered-chan n   xform exh name a/chan))
  ([n xform exh name ch]
   (let [b (a/buffer n)
         c (ch b)]
     (BufferedChan. name c b n))))

(def bchan buffered-chan)
(defn nchan
  ([] (nchan *default-length* (uuid)))
  ([name] (nchan *default-length* name))
  ([name n]       (bchan n nil nil name))
  ([name n xform] (bchan n xform nil name)))
