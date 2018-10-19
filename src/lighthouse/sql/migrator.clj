(ns lighthouse.sql.migrator
  (:require [clojure.string :as string]
            [lighthouse.util :as util :refer [snake-case rel? col?]]))

(defn not-null [m]
  (when (false? (:db/nil? m))
    "not null"))

(defn col-default [m]
  (when (contains? m :db/default)
    (str "default " (get m :db/default))))

(defn col-name [m]
  (-> m :db/col util/col-name))

(defn col [m]
  (->> [(col-name m)
        (:db/type m)
        (not-null m)
        (col-default m)]
       (filter some?)
       (string/join " ")))

(defn add-column [m]
  (let [table (-> m :db/col namespace snake-case)]
    (str "alter table " table " add column " (col m))))

(defn add-unique-index [m]
  (let [table (-> m :db/col namespace snake-case)]
    (str "create unique index idx_" table "_" (col-name m) " on " table " (" (col-name m) ")")))

(defn unique? [m]
  (true? (:db/unique? m)))

(defn one? [m]
  (and (rel? m)
       (= :one (:db/type m))))

(defn many? [m]
  (and (rel? m)
       (= :many (:db/type m))))

(defn name* [val]
  (when (ident? val)
    (name val)))

(defn rel [{:db/keys [rel ref delete]}]
  (let [on-delete (str "on delete " (or delete "cascade"))]
    (str (-> rel name snake-case) " integer references " (-> ref namespace snake-case) "(" (-> ref name snake-case) ") " on-delete)))

(defn add-rel [m]
  (let [table (-> m :db/rel namespace snake-case)]
    (str "alter table " table " add column " (rel m))))

(defn create-table-if-not-exists [table]
  (str "create table if not exists " table " ("
       " id integer primary key,"
       " updated_at timestamp,"
       " created_at timestamp not null default current_timestamp"
       " )"))

(defn rename? [m]
  (and (contains? m :db/id)
       (some? (:db/col m))))

(defn rename-column-statement [{:db/keys [col id]}]
  (let [table (-> id namespace snake-case)
        old-col (-> id name snake-case)
        new-col (-> col name snake-case)]
    (str "alter table " table " rename column " old-col " to " new-col)))

(defn migrate [v]
  (let [ks (concat (->> (filter col? v)
                        (map :db/col))
                   (->> (filter one? v)
                        (map :db/rel)))
        tables (->> (map namespace ks)
                    (map snake-case)
                    (distinct))
        table-statements (map create-table-if-not-exists tables)
        add-column-statements (->> (filter col? v)
                                   (map add-column))
        add-rel-statements (->> (filter one? v)
                                (map add-rel))
        rename-col-statements (->> (filter rename? v)
                                   (map rename-column-statement))
        add-unique-indexes (->> (filter unique? v)
                                (map add-unique-index))]
    (filter some?
      (concat table-statements
              add-column-statements
              add-rel-statements
              rename-col-statements
              add-unique-indexes))))
