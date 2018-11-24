(ns lighthouse.pg
  (:require [lighthouse.util :refer [snake-case]]))

(defn create-table [table]
  (str "create table if not exists " table " ("
       " id serial primary key,"
       " updated_at timestamptz,"
       " created_at timestamptz not null default now()"
       ")"))

(defn alter? [m]
  (contains? m :db/id))

(defn table [k]
  (when (qualified-ident? k)
    (-> k namespace snake-case)))

(defn rename-column-statement [{:db/keys [col id]}]
  (when (some? col)
    (let [table (table id)
          old-col (-> id name snake-case)
          new-col (-> col name snake-case)]
      (str "alter table " table " rename column " old-col " to " new-col))))

(defn alter-column-statement [{:db/keys [type using id]}]
  (when (some? type)
    (let [table (table id)
          using (if (some? using)
                  (str "using " using)
                  "")]
      (str "alter table " table " alter column " (-> id name snake-case) " type " type " " using))))
