(ns lighthouse.migrator
  (:require [lighthouse.sql.migrator :as sql]
            [lighthouse.util :refer [col? rel?]]
            [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn])
  (:import (org.sqlite SQLiteException)))

(defn migrations [c]
  (jdbc/query c ["select * from coast_schema_migrations"]))

(defn mapify-schema [m]
  (cond
    (col? m) {(:db/col m) m}
    (rel? m) {(:db/rel m) m}
    :else m))

(defn schema [c]
  (->> (map :migration (migrations c))
       (map edn/read-string)
       (mapcat identity)
       (map mapify-schema)
       (apply merge)))

(defn migrate [c migration]
  (let [statements (sql/migrate migration)]
    (try
      (jdbc/with-db-transaction [conn c]
        (jdbc/execute! conn "create table if not exists coast_schema_migrations (migration text unique not null, created_at timestamp not null default current_timestamp)")
        (jdbc/execute! conn "create table if not exists coast_schema (schema text unique not null, updated_at timestamp not null default current_timestamp)")
        (doall
          (for [s statements]
            (jdbc/execute! conn s)))
        (jdbc/insert! conn :coast_schema_migrations {:migration (str migration)})
        (jdbc/execute! conn ["insert or replace into coast_schema (schema)
                              values (?)" (str (schema conn))]))
      (catch SQLiteException e
        (when (and (not= "[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: coast_schema_migrations.migration)" (.getMessage e)))
          (throw e))))))
