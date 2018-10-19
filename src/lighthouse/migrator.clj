(ns lighthouse.migrator
  (:require [lighthouse.sql.migrator :as sql]
            [clojure.java.jdbc :as jdbc])
  (:import (java.security MessageDigest)
           (java.math BigInteger)
           (org.sqlite SQLiteException)))

(defn sha1 [s]
  (let [hashed
        (doto (MessageDigest/getInstance "SHA-1")
          (.reset)
          (.update (.getBytes s "UTF-8")))]
    (format "%040x" (new BigInteger 1 (.digest hashed)))))

(defn migrate [c migration]
  (let [s (sql/migrate migration)]
    (try
      (jdbc/with-db-transaction [conn c]
        (jdbc/execute! conn "create table if not exists schema_migrations (sha1 text unique not null, created_at timestamp not null default current_timestamp);")
        (jdbc/execute! conn s)
        (jdbc/insert! conn :schema_migrations {:sha1 (sha1 (str migration))}))
      (catch SQLiteException e
        (when (not= "[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: schema_migrations.sha1)" (.getMessage e))
          (throw e))))))

(comment
  (def conn (lighthouse.core/connect "dev.db"))

  (jdbc/query conn ["select * from schema_migrations"])

  (let [mig1 [{:db/col :person/name :db/type "text" :db/unique? true :db/nil? false}
              {:db/rel :person/todos :db/type :many :db/ref :todo/person}
              {:db/rel :todo/person :db/type :one :db/ref :person/id}
              {:db/col :todo/name :db/type "text"}
              {:db/col :todo/done :db/type "boolean" :db/default false :db/nil? false}]

        mig2 [{:db/col :todo/done-at :db/type "timestamp" :db/nil? false}]]

    (migrate conn mig2)

    (jdbc/query conn ["select * from schema_migrations"])))
