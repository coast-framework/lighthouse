(ns lighthouse.core
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn]
            [clojure.data.json :as json]
            [clojure.walk :as walk]
            [clojure.instant :as instant]
            [lighthouse.migrator :as migrator]
            [lighthouse.sql :as sql]
            [lighthouse.util :refer [wrap kebab-case]])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource))
  (:import (java.time Instant)
           (java.text SimpleDateFormat))
  (:refer-clojure :exclude [update]))

(def opts {:auto-commit        true
           :read-only          false
           :connection-timeout 30000
           :validation-timeout 5000
           :idle-timeout       600000
           :max-lifetime       1800000
           :minimum-idle       10
           :maximum-pool-size  10
           :register-mbeans    false})

(defn pool [s m]
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

(defn disconnect [^HikariDataSource ds]
  (.close ds))

(defn migrate [c migration]
  (migrator/migrate c migration))

(defn schema [c]
  (migrator/schema c))

(defn transact
  ([conn query params]
   (let [schema (schema conn)
         sql (sql/sql-vec schema query {})]
     (jdbc/execute! conn sql)))
  ([conn query]
   (transact conn query {})))

(defn insert [conn val]
  (let [v (wrap val)
        cols (->> (mapcat identity v)
                  (map first)
                  (distinct))
        values (map #(map (fn [x] (second x)) %) v)]
    (transact conn (concat
                    (conj cols :insert)
                    (conj values :values)))))

(defn update [conn val]
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

(defn delete [conn val]
  (let [v (wrap val)
        table (-> v first keys first namespace)
        pk (-> v first ffirst)]
    (transact conn [:delete
                    :from table
                    :where [pk (map #(get % pk) v)]])))

(defn qualify-col [s]
  (let [parts (string/split s #"\$")
        k-ns (first (map #(string/replace % #"_" "-") parts))
        k-n (->> (rest parts)
                 (map #(string/replace % #"_" "-"))
                 (string/join "-"))]
    (keyword k-ns k-n)))

(defn parse-json [schema val]
  (if (and (sequential? val)
           (= 2 (count val))
           (or (= :many (get-in schema [(first val) :db/type]))
               (= :one (get-in schema [(first val) :db/type])))
           (string? (second val)))
    [(first val) (json/read-str (second val) :key-fn qualify-col)]
    val))

(defn coerce-inst [val]
  (if (string? val)
    (try
      (instant/read-instant-timestamp val)
      (catch Exception e
        val))
    val))

(defn coerce-timestamp-inst [val]
  (if (string? val)
    (try
      (let [fmt (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
        (.parse fmt val))
      (catch Exception e
        val))
    val))

(defn q
  ([conn v params]
   (let [schema (schema conn)
         sql (sql/sql-vec schema v params)]
     (vec
      (walk/postwalk coerce-timestamp-inst
       (walk/postwalk coerce-inst
        (walk/prewalk #(parse-json schema %)
         (jdbc/query conn sql {:keywordize? false
                               :identifiers qualify-col})))))))
  ([conn v]
   (q conn v {})))

(defn pull [conn v where-clause]
  (first
    (q conn [:pull v
             :where where-clause])))
