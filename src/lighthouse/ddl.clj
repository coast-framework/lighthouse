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


(defn col-type [type {:keys [precision scale]}]
  (if (or (some? precision)
          (some? scale))
    (str (or (util/sqlize type) "numeric")
         (when (or (some? precision)
                   (some? scale))
           (str "(" (string/join ","
                      (filter some? [(or precision 0) scale]))
                ")")))
    (util/sqlize type)))


(defn col [type col-name m]
  "SQL fragment for adding a column in create or alter table"
  (->> [(util/sqlize col-name)
        (col-type type m)
        (unique m)
        (collate m)
        (not-null m)
        (col-default m)]
       (filter some?)
       (string/join " ")
       (string/trim)))


(defn add-column
  "SQL for adding a column to an existing table"
  [table col-name type & {:as m}]
  (str "alter table " (util/sqlize table) " add column " (col type col-name m)))


(defn on-delete [m]
  (when (contains? m :on-delete)
    (str "on delete " (util/sqlize (:on-delete m)))))


(defn on-update [m]
  (when (contains? m :on-update)
    (str "on update " (util/sqlize (:on-update m)))))


(defn add-foreign-key
  "SQL for adding a foreign key column to an existing table"
  [from to & {col :col pk :pk fk-name :name :as m}]
  (let [from (util/sqlize from)
        to (util/sqlize to)]
   (string/join " "
     (filter some?
       ["alter table"
        from
        "add constraint"
        (or (util/sqlize fk-name) (str from "_" to "_fk"))
        "foreign key"
        (str "(" (or (util/sqlize col) to) ")")
        "references"
        to
        (str "(" (or (util/sqlize pk) "id") ")")
        (on-delete m)
        (on-update m)]))))


(defn where [m]
  (when (contains? m :where)
    (str "where " (:where m))))


(defn index-cols [cols {order :order}]
  (->> (map #(conj [%] (get order %)) cols)
       (map #(map util/sqlize %))
       (map #(string/join " " %))
       (map string/trim)))

(defn add-index
  "SQL for adding an index to an existing table"
  [table-name cols & {:as m}]
  (let [table-name (util/sqlize table-name)
        cols (if (sequential? cols)
               cols
               [cols])
        cols (index-cols cols m)
        col-name (string/join ", " cols)
        index-col-names (map #(string/replace % #" " "_") cols)
        index-name (or (:name m) (str table-name "_" (string/join "_" index-col-names) "_index"))]
    (string/join " "
      (filter some?
        ["create"
         (unique m)
         "index"
         index-name
         "on"
         table-name
         (str "(" col-name ")")
         (where m)]))))


(defn add-reference
  "SQL for adding a foreign key column to an existing table"
  [table-name ref-name & {:as m}]
  (string/join " "
    (filter some?
      ["alter table"
       (util/sqlize table-name)
       "add column"
       (util/sqlize ref-name)
       (or (-> m :type util/sqlize) "integer")
       "references"
       (util/sqlize ref-name)
       (str "(id)")])))


(defn alter-column [table-name col-name type & {:as m}]
  (string/join " "
    (filter some?
      ["alter table"
       (util/sqlize table-name)
       "alter column"
       (util/sqlize col-name)
       "type"
       (util/sqlize type)
       (when (contains? m :using)
        (str "using " (:using m)))])))


(defn text [col-name & {:as m}]
  (col :text col-name m))


(defn decimal [col-name & {:as m}]
  (col :decimal col-name m))


(defn create-table
  "SQL to create a table"
  [dialect table & args]
  (let [args (if (sequential? args) args '())])
  (string/join " "
    (filter some?
      [(str "create table " (util/sqlize table) " (")
       (string/join ", "
        (conj args (str "id " (get-in sql [dialect :pk]))))
       ")"])))


(defn create-extension [s]
  (str "create extension " s))


(defn drop-extension [s]
  (str "drop extension " s))


(defn drop-table [table]
  (str "drop table " (util/sqlize table)))


(defn drop-column
  "SQL for dropping a column from a table"
  [table col]
  (str "alter table " (util/sqlize table) " drop column " (util/sqlize col)))


(defn drop-foreign-key [alter-table-name & {:as m}]
  (let [constraint (when (contains? m :table)
                     (util/sqlize (:table m)) "_" (util/sqlize alter-table-name) "_fkey")
        constraint (if (contains? m :name)
                     (util/sqlize (:name m))
                     constraint)]
    (str "alter table " (util/sqlize alter-table-name) " drop constraint " constraint)))


(defn drop-index [table-name & {cols :column :as m}]
  (let [cols (if (sequential? cols) cols [cols])
        cols (index-cols cols m)
        col-name (string/join ", " cols)
        index-col-names (map #(string/replace % #" " "_") cols)
        index-name (or (-> m :name util/sqlize) (str table-name "_" (string/join "_" index-col-names) "_index"))]
    (str "drop index " index-name)))


(defn drop-reference [table-name ref-name]
  (str "alter table "
       (util/sqlize table-name)
       " drop constraint "
       (util/sqlize ref-name) "_" (util/sqlize table-name) "_fkey"))


(defn rename-column [table-name column-name new-column-name]
  (string/join " "
    ["alter table"
     (util/sqlize table-name)
     "rename column"
     (util/sqlize column-name)
     "to"
     (util/sqlize new-column-name)]))


(defn rename-index [index-name new-index-name]
  (string/join " "
    ["alter index"
     index-name
     "rename to"
     new-index-name]))


(defn rename-table [table-name new-table-name]
  (string/join " "
    ["alter table"
     table-name
     "rename to"
     new-table-name]))


(defn timestamps [dialect]
  (string/join " "
    [(str "updated_at " (get-in sql [dialect :timestamp]) ",")
     (str "created_at " (get-in sql [dialect :timestamp]) " not null default " (get-in sql [dialect :now]))]))
