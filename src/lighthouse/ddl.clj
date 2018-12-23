(ns lighthouse.ddl
  (:require [lighthouse.util :as util]
            [clojure.edn :as edn]
            [clojure.string :as string]))


(def sql {:sqlite {:timestamp "timestamp"
                   :now "current_timestamp"
                   :pk "integer primary key"}
          :pg {:timestamp "timestamptz"
               :now "now()"
               :pk "serial primary key"}})


(defn add-column
  "SQL for adding a column to an existing table"
  [table col type]
  (str "alter table " (util/sqlize table) " add column " (util/sqlize col) " " (name type)))


(defn add-foreign-key [])
(defn add-index [])
(defn add-reference [])
(defn add-timestamps [])
(defn change-column [])


(defn mappable? [coll]
  (and (sequential? coll)
       (even? (count coll))))


(defn seq->map [coll]
  (if (not (mappable? coll))
    (throw (Exception. (str coll " requires pairs of elements. Example: (:a 1 :b 2) not (:a 1 :b)")))
    (->> (partition 2 coll)
         (map vec)
         (into {}))))


(defn not-null [m]
  (when (false? (:null m))
    "not null"))


(defn col-default [m]
  (when (contains? m :default)
    (str "default " (get m :default))))


(defn unique [m]
  (when (true? (:unique m))
    (str "unique")))


(defn collate [m]
  (when (contains? m :collate)
    (str "collate " (get m :collate))))


(defn text [col & args]
  (let [m (seq->map args)]
    (->> [(util/sqlize col)
          "text"
          (unique m)
          (collate m)
          (not-null m)
          (col-default m)]
         (filter some?)
         (string/join " ")
         (string/trim))))


(defn create-table [dialect table & args]
  (string/join " "
    (filter some?
      [(str "create table " (util/sqlize table) " (")
       (str "id " (get-in sql [dialect :pk]) ",")
       (when (not (empty? args))
         (str (string/join ", " args) ","))
       (str "updated_at " (get-in sql [dialect :timestamp]) ",")
       (str "created_at " (get-in sql [dialect :timestamp]) " not null default " (get-in sql [dialect :now]))
       ")"])))


(defn create-extension [])
(defn drop-extension [])


(defn drop-table [table]
  (str "drop table " (util/sqlize table)))


(defn drop-column
  "SQL for dropping a column from a table"
  [table col]
  (str "alter table " (util/sqlize table) " drop column " (util/sqlize col)))


(defn drop-foreign-key [])
(defn drop-index [])
(defn drop-reference [])
(defn drop-timestamps [])
(defn rename-column [])
(defn rename-index [])
(defn rename-table [])

(defn timestamps [])
