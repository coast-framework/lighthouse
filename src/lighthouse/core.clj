(ns lighthouse.core
  (:require [clojure.string :as string]
            [lighthouse.migrator :as migrator])
  (:import (com.zaxxer.hikari HikariConfig HikariDataSource)))

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
            (.setConnectionTestQuery "SELECT 1")
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
