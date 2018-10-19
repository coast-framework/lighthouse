(ns lighthouse.sql.insert
  (:require [lighthouse.util :refer [kebab-case snake-case col-name table unqualify-keys]]
            [clojure.string :as string]))

(defn insert-into [table tuple]
  (str "insert into " table " ("
       (->> (map first tuple)
            (map name)
            (string/join ", "))
      ")"))

(defn values [t]
  (str "values " (string/join ","
                  (map #(str "(" (->> (map (fn [_] "?") %)
                                      (string/join ","))
                            ")")
                       t))))

(defn tuple [v]
  (mapv identity v))

(defn resolve-rel [[k v]]
  (if (map? v)
    [k (get v (keyword (name k) "id"))]
    [k v]))

(defn resolve-rels [val]
  (mapv resolve-rel val))

(defn sql-vec [arg]
  (let [v (if (sequential? arg) arg [arg])
        table (table (first v))
        un-v (map unqualify-keys v)
        tuples (->> (map tuple un-v)
                    (map resolve-rels))
        insert-statement (insert-into table (first tuples))
        values-statement (values tuples)]
    (vec (concat [(->> (filter some? [insert-statement values-statement])
                       (string/join " "))]
                 (mapcat #(map second %) tuples)))))
