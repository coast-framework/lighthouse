(ns lighthouse.sql.update
  (:require [clojure.string :as string]
            [lighthouse.util :refer [col-name table unqualify-keys]])
  (:import (java.time Instant)))

(defn update-statement [table tuples]
  (str "update " table " set "
       (->> (map (fn [[k _]] (str (col-name k) " = ?")) tuples)
            (string/join ", "))))

(defn sql-vec [m]
  (let [table (table m)
        un-m (unqualify-keys m)
        params (dissoc un-m :id)
        tuples (map identity params)
        update-statement (update-statement table tuples)
        where-clause "where id = ?"]
    (vec (concat [(string/join " " (filter some? [update-statement where-clause]))]
                 (map second tuples)
                 [(get un-m :id)]))))

(defn sql-vecs [arg]
  (let [v (if (sequential? arg) arg [arg])
        v (mapv #(assoc % :updated-at (Instant/now)) v)]
    (map sql-vec v)))
