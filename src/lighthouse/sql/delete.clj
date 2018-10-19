(ns lighthouse.sql.delete
  (:require [lighthouse.util :refer [unqualify-keys table]]
            [clojure.string :as string]))

(defn ? [m]
  (->> (keys m)
       (map (fn [_] (str "?")))
       (string/join ", ")))

(defn sql-vec [val]
  (let [v (if (sequential? val) val [val])
        table (table (first v))
        sql (str "delete from " table " where id in (" (string/join ", " (map ? v)) ")")]
    (vec (apply concat [sql] (map #(-> % vals) v)))))
