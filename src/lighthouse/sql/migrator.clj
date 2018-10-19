(ns lighthouse.sql.migrator
  (:require [clojure.string :as string]
            [lighthouse.util :refer [snake-case]]))

(defn col? [m]
  (contains? m :db/col))

(defn not-null [m]
  (when (false? (or (:db/nil? m)
                    (:db/nil m)))
    "not null"))

(defn col-default [m]
  (when (contains? m :db/default)
    (str "default " (get m :db/default))))

(defn col-name [m]
  (-> m :db/col name snake-case))

(defn col [m]
  (->> [(col-name m)
        (:db/type m)
        (not-null m)
        (col-default m)]
       (filter some?)
       (string/join " ")))

(defn add-column [m]
  (let [table (-> m :db/col namespace snake-case)
        unique (if (or (contains? m :db/unique?)
                       (contains? m :db/unique))
                  (str "create unique index idx_" table "_" (col-name m) " on " table " (" (col-name m) ")")
                  nil)
        statements [(str "alter table " table " add column " (col m))
                    unique]]
    (string/join ";\n"
      (filter some? statements))))

(defn rel? [m]
  (contains? m :db/rel))

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
    (str (-> rel name snake-case) " integer not null references " (-> ref namespace snake-case) "(" (-> ref name snake-case) ") " on-delete)))

(defn add-rel [m]
  (let [table (-> m :db/rel namespace snake-case)]
    (str "alter table " table " add column " (rel m))))

(defn create-table-if-not-exists [table]
  (str "create table if not exists " table " ("
       " id integer primary key,"
       " updated_at timestamp,"
       " created_at timestamp not null default current_timestamp"
       " )"))

(defn drop? [m]
  (and (contains? m :db/id)
       (nil? (:db/col m))))

(defn drop-column-statement [k]
  (let [table (-> k namespace snake-case)
        col (-> k name snake-case)]
    (str "alter table " table " drop column " col)))

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
        drop-col-statements (->> (filter drop? v)
                                 (map drop-column-statement))]
    (string/join "; "
      (concat table-statements
              add-column-statements
              add-rel-statements
              rename-col-statements
              drop-col-statements))))
