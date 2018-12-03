(ns lighthouse.core
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [clojure.java.shell :as shell]
            [lighthouse.migrator :as migrator]
            [lighthouse.sql :as sql]
            [lighthouse.defq :refer [query-fns]]
            [lighthouse.util :refer [db pg? sqlite? wrap kebab-case table snake-case]]
            [lighthouse.transact :as lh.transact])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.time Instant)
           (java.text SimpleDateFormat)
           (java.util TimeZone))
  (:refer-clojure :exclude [update drop])
  (:gen-class))

(def opts {:auto-commit        true
           :read-only          false
           :connection-timeout 30000
           :validation-timeout 5000
           :idle-timeout       600000
           :max-lifetime       1800000
           :minimum-idle       10
           :maximum-pool-size  10
           :register-mbeans    false})

(defn driver-class-name [s]
  "Determines which driver class to pass into the hikari cp config"
  (when (string? s)
    (cond
      (sqlite? s) "org.sqlite.JDBC"
      (pg? s) "org.postgresql.Driver"
      :else nil)))

(defn pool
  "Shamelessly stolen from hikari-cp and makes a new hikaricp data source"
  [s m]
  (let [m (merge opts m)
        driver-class-name (driver-class-name s)
        _ (when (nil? driver-class-name)
            (throw (Exception. "Unsupported connection string, only sqlite and postgres are supported currently")))
        c (doto (HikariConfig.)
            (.setDriverClassName     driver-class-name)
            (.setJdbcUrl             s)
            (.setAutoCommit          (:auto-commit m))
            (.setReadOnly            (:read-only m))
            (.setConnectionTimeout   (:connection-timeout m))
            (.setValidationTimeout   (:validation-timeout m))
            (.setIdleTimeout         (:idle-timeout m))
            (.setMaxLifetime         (:max-lifetime m))
            (.setMinimumIdle         (:minimum-idle m))
            (.setMaximumPoolSize     (:maximum-pool-size m)))]
    {:datasource (HikariDataSource. c)}))

(defn connect
  "Takes a string returns a connection pool from a connection string"
  ([s m]
   (if (string/blank? s)
     (throw (Exception. "You need to give lighthouse.core/connection a valid connection string"))
     (pool s m)))
  ([s]
   (connect s {})))

(defn disconnect
  "Takes a lighthouse {:datasource ^HikariDataSource} map and closes the connection to the database"
  [m]
  (.close (:datasource m)))

(defn migrate
  "Takes a jdbc connection or datasource and a migration vector"
  ([c migration m]
   (migrator/migrate c migration m))
  ([c migration]
   (migrate c migration {})))

(defn rollback
  "Rolls back a migration vector"
  ([c m]
   (migrator/rollback c m))
  ([c]
   (rollback c {})))

(defn schema
  "Shows the current lighthouse schema"
  [c]
  (migrator/schema c))

(defn transact
  "Takes a jdbc connection, a query and an optional map of params

      Ex: (transact conn '[:delete
                           :from todo
                           :where [todo/id 1]])
  "
  ([conn query params]
   (let [schema (schema conn)
         db (db conn)
         sql (sql/sql-vec db schema query {})]
     (jdbc/execute! conn sql)))
  ([conn query]
   (transact conn query {})))

(defn insert
  "Inserts a map or a vector of maps into the db. All maps need to have the same keys."
  [conn val]
  (transact conn (lh.transact/insert val)))

(defn update
  "Updates a map or a vector of maps in the db. All maps need to have one primary key and the same set of keys."
  [conn val]
  (transact conn (lh.transact/update val)))

(defn delete
  "Deletes a map or a vector of maps by primary key only"
  [conn val]
  (transact conn (lh.transact/delete val)))

(defn qualify-col
  "This is a relatively complex function to replace col names with $ in them, pull comes at a cost"
  [s]
  (let [parts (string/split s #"\$")
        k-ns (-> (first parts) (string/replace #"_" "-"))
        k-n (->> (rest parts)
                 (map #(string/replace % #"_" "-"))
                 (string/join "-"))]
    (keyword k-ns k-n)))

(defn parse-json
  "Parses json from pull queries"
  [schema val]
  (if (and (sequential? val)
           (= 2 (count val))
           (or (= :many (get-in schema [(first val) :db/type]))
               (= :one (get-in schema [(first val) :db/type])))
           (string? (second val)))
    [(first val) (json/read-str (second val) :key-fn qualify-col)]
    val))

(defn one-first
  "Get the first value from a one rel (since it's a vector)"
  [schema val]
  (if (and (sequential? val)
           (= :one (get-in schema [(first val) :db/type]))
           (sequential? (second val))
           (= 1 (count (second val))))
    [(first val) (first (second val))]
    val))

(defn coerce-inst
  "Coerce json iso8601 to clojure #inst"
  [val]
  (if (string? val)
    (try
      (let [fmt (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss'Z'")
            _ (.setTimeZone fmt (TimeZone/getTimeZone "UTC"))]
        (.parse fmt val))
      (catch Exception e
        val))
    val))

(defn coerce-timestamp-inst
  "Coerce timestamps to clojure #inst"
  [val]
  (if (string? val)
    (try
      (let [fmt (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")
            _ (.setTimeZone fmt (TimeZone/getTimeZone "UTC"))]
        (.parse fmt val))
      (catch Exception e
        val))
    val))

(defn coerce-boolean
  "Coerce sqlite booleans: 1 or 0 to clojure booleans"
  [schema val]
  (if (and (vector? val)
           (= 2 (count val))
           (= "boolean" (get-in schema [(first val) :db/type])))
    [(first val) (= 1 (second val))]
    val))

(defn q
  "The main entry point for queries

    Ex: (q conn '[:select todo/*])
  "
  ([conn v params]
   (let [schema (schema conn)
         db (db conn)
         sql (sql/sql-vec db schema v params)]
     (vec
      (walk/postwalk #(-> % coerce-inst coerce-timestamp-inst ((partial coerce-boolean schema)))
        (walk/prewalk #(->> (parse-json schema %) (one-first schema))
         (jdbc/query conn sql {:keywordize? false
                               :identifiers qualify-col}))))))
  ([conn v]
   (q conn v {})))

(defn pull
  "The main entry point for queries that return a nested result based on a lighthouse schema and a primary key"
  [conn v where-clause]
  (first
    (q conn [:pull v
             :where where-clause])))

(defn defq
  "Use regular .sql files and call them as functions

  Ex:

  test.sql:
  -- name: some-sql
  select *
  from todo
  where todo.name = :name

  clojure:
  (defq \"test.sql\")

  creates fn some-sql in the current namespace

  (some-sql {:name \"test todo #1\"})
  "
  [conn filename]
  (query-fns ~conn ~filename))

(defn create
 "Creates a new database"
 [s]
 (let [m (shell/sh "createdb" s)]
   (if (= 0 (:exit m))
     (str s " created successfully")
     (:err m))))

(defn drop
  "Drops an existing database"
  [s]
  (let [m (shell/sh "dropdb" s)]
    (if (= 0 (:exit m))
      (str s " dropped successfully")
      (:err m))))

(defn cols
  "Lists the columns in the db given the table name as keyword

  Ex:

  (cols :table-name) ; => corresponds to postgres table table_name
  "
  [c k]
  (let [table (-> k name snake-case)
        rows (cond
               (sqlite? c) (jdbc/query c ["select m.name || '/' || p.name as column_name,
                                           p.type as data_type
                                           from sqlite_master m
                                           left outer join pragma_table_info((m.name)) p
                                                on m.name <> p.name
                                           where m.name = ?
                                           order by m.name, column_name" table])
               (pg? c) (jdbc/query c ["select table_name || '/' || column_name as column_name,
                                       data_type
                                       from information_schema.columns
                                       where table_name = ?
                                       order by table_name, column_name" table])
               :else [])]
    (->> (map #(vector (-> % :column_name keyword) (% :data_type)) rows)
         (into {}))))
