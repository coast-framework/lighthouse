(ns lighthouse.core
  (:require [clojure.string :as string]
            [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn]
            [lighthouse.migrator :as migrator]
            [lighthouse.sql.insert :as sql.insert]
            [lighthouse.sql.update :as sql.update]
            [lighthouse.sql.delete :as sql.delete])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource))
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
     (throw (Exception. "You need to give lighthouse.core/connection a jdbc connection string"))
     (pool s m)))
  ([s]
   (connect s {})))

(defn disconnect [^HikariDataSource ds]
  (.close ds))

(defn migrate [c migration]
  (migrator/migrate c migration))

(defn schema [c]
  (migrator/schema c))

(defn insert [c val]
  (let [v (sql.insert/sql-vec val)]
    (jdbc/execute! c v)))

(defn update [c val]
  (let [vecs (sql.update/sql-vec val)]
    (jdbc/with-db-transaction [conn c]
      (doall
        (for [v vecs]
          (jdbc/execute! conn v))))))

(defn delete [c val]
  (let [v (sql.delete/sql-vec val)]
    (jdbc/execute! c v)))
