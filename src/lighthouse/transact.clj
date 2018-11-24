(ns lighthouse.transact
  (:require [lighthouse.util :refer [wrap]])
  (:refer-clojure :exclude [update]))

(defn insert
  "Inserts a map or a vector of maps into the db. All maps need to have the same keys."
  [val]
  (let [v (wrap val)
        cols (->> (mapcat identity v)
                  (map first)
                  (distinct))
        values (map #(map (fn [x] (second x)) %) v)]
    (concat
     (conj cols :insert)
     (conj values :values))))

(defn update
  "Updates a map or a vector of maps in the db. All maps need to have one primary key and the same set of keys."
  [val]
  (let [v (wrap val)
        table (-> v first keys first namespace)
        pk (first (filter #(= "id" (name %)) (-> v first keys)))]
    (concat
      [:update table
       :where [(name pk) (map #(get % pk) v)]]
      (conj (->> (mapcat identity v)
                 (distinct))
            :set))))

(defn delete
  "Deletes a map or a vector of maps by primary key only"
  [val]
  (let [v (wrap val)
        table (-> v first keys first namespace)
        pk (-> v first ffirst)]
    [:delete
     :from table
     :where [pk (map #(get % pk) v)]]))
