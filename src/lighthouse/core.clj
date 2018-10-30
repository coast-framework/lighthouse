(ns lighthouse.core
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [clojure.instant :as instant]
            [lighthouse.migrator :as migrator]
            [lighthouse.sql :as sql]
            [lighthouse.defq :refer [query create-root-var query-fn query-fns]]
            [lighthouse.util :refer [wrap kebab-case]])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)
           (java.time Instant)
           (java.text SimpleDateFormat))
  (:refer-clojure :exclude [update])
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

(defn pool
  "Shamelessly stolen from hikari-cp and makes a new hikaricp data source"
  [s m]
  (let [m (merge opts m)
        c (doto (HikariConfig.)
            (.setDriverClassName     "org.sqlite.JDBC")
            (.setJdbcUrl             (str "jdbc:sqlite:" s))
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
  "Takes a string returns a connection pool from a sqlite connection string"
  ([s m]
   (if (string/blank? s)
     (throw (Exception. "You need to give lighthouse.core/connection the path to your db file string"))
     (pool s m)))
  ([s]
   (connect s {})))

(defn disconnect
  "Takes a java hikaricp source and closes the connection to the database"
  [^HikariDataSource ds]
  (.close ds))

(defn migrate
  "Takes a jdbc connection or datasource and a migration vector"
  [c migration]
  (migrator/migrate c migration))

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
         sql (sql/sql-vec schema query {})]
     (jdbc/execute! conn sql)))
  ([conn query]
   (transact conn query {})))

(defn insert
  "Inserts a map or a vector of maps into the db. All maps need to have the same keys."
  [conn val]
  (let [v (wrap val)
        cols (->> (mapcat identity v)
                  (map first)
                  (distinct))
        values (map #(map (fn [x] (second x)) %) v)]
    (transact conn (concat
                    (conj cols :insert)
                    (conj values :values)))))

(defn update
  "Updates a map or a vector of maps in the db. All maps need to have one primary key and the same set of keys."
  [conn val]
  (let [v (wrap val)
        table (-> v first keys first namespace)
        pk (first (filter #(= "id" (name %)) (-> v first keys)))]
    (transact conn
      (concat
        [:update table
         :where [(name pk) (map #(get % pk) v)]]
        (conj (->> (map #(dissoc % pk) v)
                   (mapcat identity)
                   (distinct))
              :set)))))

(defn delete
  "Deletes a map or a vector of maps by primary key only"
  [conn val]
  (let [v (wrap val)
        table (-> v first keys first namespace)
        pk (-> v first ffirst)]
    (transact conn [:delete
                    :from table
                    :where [pk (map #(get % pk) v)]])))

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

(defn coerce-inst
  "Coerce json iso8601 to clojure #inst"
  [val]
  (if (string? val)
    (try
      (instant/read-instant-timestamp val)
      (catch Exception e
        val))
    val))

(defn coerce-timestamp-inst
  "Coerce timestamps to clojure #inst"
  [val]
  (if (string? val)
    (try
      (let [fmt (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
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
         sql (sql/sql-vec schema v params)]
     (vec
      (walk/postwalk #(-> % coerce-inst coerce-timestamp-inst ((partial coerce-boolean schema)))
        (walk/prewalk #(parse-json schema %)
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

(defmacro defq
  ([conn n filename]
   `(let [m (-> (query ~(str n) ~filename)
                (assoc :ns *ns*))
          q-fn# (query-fn ~conn m)]
      (create-root-var ~(str n) q-fn#)))
  ([conn filename]
   `(query-fns ~conn ~filename)))
