(ns lighthouse.migrator
  (:require [lighthouse.sql.migrator :as sql]
            [lighthouse.util :refer [col? rel? map-vals kebab-case]]
            [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn])
  (:import (org.sqlite SQLiteException)))

(defn migrations [c]
  (jdbc/with-db-transaction [conn c]
    (jdbc/execute! conn "create table if not exists coast_schema_migrations (migration text unique not null, created_at timestamp not null default current_timestamp)")
    (jdbc/query conn ["select * from coast_schema_migrations"])))

(defn db-schema [c]
  (jdbc/query c ["select m.name as table_name,
                         m.name || '/' || p.name as column_name
                  from sqlite_master m
                  left outer join pragma_table_info((m.name)) p
                        on m.name <> p.name
                  order by table_name, column_name"]))

(defn mapify-schema [m]
  (cond
    (col? m) {(:db/col m) m}
    (rel? m) {(:db/rel m) m}
    :else m))

(defn schema [c]
  (let [coast-schema (->> (map :migration (migrations c))
                          (map edn/read-string)
                          (mapcat identity)
                          (map mapify-schema)
                          (apply merge))
        db-schema  (map-vals #(->> (map (fn [m] (dissoc m :table_name)) %)
                                   (map :column_name)
                                   (map kebab-case)
                                   (map keyword))
                    (group-by #(-> % :table_name kebab-case keyword) (db-schema c)))]
    (merge coast-schema db-schema)))

(defn migrate [c migration]
  (let [statements (sql/migrate migration)
        migrations (set (map :migration (migrations c)))]
    (when (not (contains? migrations (str migration)))
      (try
        (jdbc/with-db-transaction [conn c]
          (jdbc/execute! conn "create table if not exists coast_schema (schema text unique not null, updated_at timestamp not null default current_timestamp)")
          (doall
            (for [s statements]
              (jdbc/execute! conn s)))
          (jdbc/insert! conn :coast_schema_migrations {:migration (str migration)})
          (jdbc/execute! conn ["insert or replace into coast_schema (schema)
                              values (?)" (str (schema conn))]))
        (catch SQLiteException e
          (when (and (not= "[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: coast_schema_migrations.migration)" (.getMessage e)))
            (throw e)))))))
