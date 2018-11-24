(ns lighthouse.migrator
  (:require [lighthouse.util :as util :refer [db pg? sqlite? col? one? rel? map-vals kebab-case snake-case]]
            [lighthouse.pg :as pg]
            [clojure.java.jdbc :as jdbc]
            [clojure.edn :as edn]
            [clojure.string :as string]))

(def sql {:sqlite {:timestamp "timestamp"
                   :now "current_timestamp"
                   :pk "integer primary key"}
          :pg {:timestamp "timestamptz"
               :now "now()"
               :pk "serial primary key"}})

(defn migrations [c]
  (let [db (db c)]
    (jdbc/with-db-transaction [conn c]
      (jdbc/execute! conn (str "create table if not exists coast_migrations (migration text unique not null, created_at " (-> sql db :timestamp) " not null default "  (-> sql db :now) ")"))
      (jdbc/query conn ["select * from coast_migrations"]))))

(defn db-schema [c]
  (cond
    (sqlite? c) (jdbc/query c ["select m.name as table_name,
                                       m.name || '/' || p.name as column_name
                                from sqlite_master m
                                left outer join pragma_table_info((m.name)) p
                                      on m.name <> p.name
                                order by table_name, column_name"])
    (pg? c) (jdbc/query c ["select
                             table_name as table_name,
                             table_name || '/' || column_name as column_name
                           from information_schema.columns
                           where table_schema not in ('pg_catalog', 'information_schema')
                           order by table_name, column_name"])
    :else []))

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

(defn not-null [m]
  (when (false? (:db/nil? m))
    "not null"))

(defn col-default [m]
  (when (contains? m :db/default)
    (str "default " (get m :db/default))))

(defn col-name [m]
  (-> m :db/col util/col-name))

(defn col [m]
  (->> [(col-name m)
        (:db/type m)
        (not-null m)
        (col-default m)]
       (filter some?)
       (string/join " ")))

(defn add-column [m]
  (let [table (-> m :db/col namespace snake-case)]
    (str "alter table " table " add column " (col m))))

(defn add-unique-index [m]
  (let [table (-> m :db/col namespace snake-case)]
    (str "create unique index idx_" table "_" (col-name m) " on " table " (" (col-name m) ")")))

(defn unique? [m]
  (true? (:db/unique? m)))

(defn many? [m]
  (and (rel? m)
       (= :many (:db/type m))))

(defn rel [{:db/keys [rel ref delete]}]
  (let [on-delete (str "on delete " (or delete "cascade"))]
    (str (-> rel name snake-case) " integer references " (-> ref namespace snake-case) "(" (-> ref name snake-case) ") " on-delete)))

(defn add-rel [m]
  (let [table (-> m :db/rel namespace snake-case)]
    (str "alter table " table " add column " (rel m))))

(defn rename? [m]
  (and (contains? m :db/id)
       (some? (:db/col m))))

(defn rename-col-statement [{:db/keys [col id]}]
  (let [table (-> id namespace snake-case)
        old-col (-> id name snake-case)
        new-col (-> col name snake-case)]
    (str "alter table " table " rename column " old-col " to " new-col)))

(defn create-table [db table]
  (let [table (-> table name snake-case)]
    (str "create table if not exists " table " ("
         "id " (-> sql db :pk) ", "
         "updated_at " (-> sql db :timestamp) ", "
         "created_at " (-> sql db :timestamp) " not null default " (-> sql db :now)
         ")")))

(defn table? [m]
  (contains? m :db/table))

(defn up? [m]
  (contains? m :db/up))

(defn down? [m]
  (contains? m :db/down))

(defn statements [conn v]
  (let [db (db conn)
        sql-statements (->> (filter up? v)
                            (map :db/up))
        ks (concat (->> (filter col? v)
                        (map :db/col))
                   (->> (filter one? v)
                        (map :db/rel)))
        table-statements (->> (map namespace ks)
                              (distinct)
                              (map #(create-table db %)))
        add-column-statements (->> (filter col? v)
                                   (map add-column))
        add-rel-statements (->> (filter one? v)
                                (map add-rel))
        rename-col-statements (->> (filter rename? v)
                                   (map rename-col-statement))
        alter-col-statements (when (pg? conn)
                               (->> (filter pg/alter? v)
                                    (map pg/alter-column-statement)))
        add-unique-indexes (->> (filter unique? v)
                                (map add-unique-index))]
    (filter some?
      (concat table-statements
              add-column-statements
              add-rel-statements
              rename-col-statements
              alter-col-statements
              add-unique-indexes
              sql-statements))))

(defn migrate [c migration m]
  (let [db (db c)
        statements (statements c migration)
        migrations (set (map :migration (migrations c)))
        schema-table (str "create table if not exists coast_schema (schema text unique not null, updated_at " (-> sql db :timestamp) " not null default "  (-> sql db :now) ")")]
    (if (true? (:dry-run? m))
      statements
      (when (not (contains? migrations (str migration)))
        (try
          (jdbc/with-db-transaction [conn c]
            (jdbc/execute! conn schema-table)
            (doall
              (for [s statements]
                (jdbc/execute! conn s)))
            (jdbc/insert! conn :coast_migrations {:migration (str migration)})
            (jdbc/execute! conn (cond
                                  (sqlite? c) ["insert or replace into coast_schema (schema) values (?)" (str (schema conn))]
                                  (pg? c) (let [schema-val (str (schema conn))]
                                            ["insert into coast_schema (schema) values (?) on conflict (schema) do update set schema = ?" schema-val schema-val])
                                  :else [])))
          (catch Exception e
            (when (not= "[SQLITE_CONSTRAINT]  Abort due to constraint violation (UNIQUE constraint failed: coast_schema_migrations.migration)" (.getMessage e))
              (throw e))))))))

(defn drop-col [m]
  (let [table (-> m :db/col namespace snake-case)]
    (str "alter table " table " drop column " (-> m :db/col name snake-case))))

(defn drop-one-rel [m]
  (let [table (-> m :db/rel namespace snake-case)]
    (str "alter table " table " drop column " (-> m :db/rel name snake-case))))

(defn rollback-statements [db v]
  (let [drop-cols (->> (filter col? v)
                       (map drop-col))
        drop-one-rels (->> (filter one? v)
                           (map drop-one-rel))
        sql (->> (filter down? v)
                 (map :db/down))]
    (filter some?
      (concat
       drop-cols
       drop-one-rels
       sql))))

(defn can-drop-table? [v]
  (= '("created-at" "id" "updated-at")
      (sort
       (map name v))))

(defn drop-table [s]
  (str "drop table " s))

(defn rollback [c m]
  (let [db (db c)
        migrations (set (map :migration (migrations c)))
        migration (-> migrations last edn/read-string)
        statements (rollback-statements db migration)
        schema-table (str "create table if not exists coast_schema (schema text unique not null, updated_at " (-> sql db :timestamp) " not null default "  (-> sql db :now) ")")]
    (if (true? (:dry-run? m))
      statements
      (when (contains? migrations (str migration))
        (jdbc/with-db-transaction [conn c]
          (jdbc/execute! conn schema-table)
          (doseq [s statements]
            (jdbc/execute! conn s))
          (jdbc/delete! conn :coast_migrations ["migration = ?" (str migration)])
          (jdbc/execute! conn (cond
                                (sqlite? c) ["insert or replace into coast_schema (schema) values (?)" (str (schema conn))]
                                (pg? c) (let [schema-val (str (schema conn))]
                                          ["insert into coast_schema (schema) values (?) on conflict (schema) do update set schema = ?" schema-val schema-val])
                                :else []))
          (let [schema (schema conn)
                drop-table-strs (->> (vals schema)
                                     (filter sequential?)
                                     (filter can-drop-table?)
                                     (map first)
                                     (map namespace)
                                     (map drop-table))]
            (doseq [s drop-table-strs]
              (jdbc/execute! conn s))
            (jdbc/execute! conn (cond
                                  (sqlite? c) ["insert or replace into coast_schema (schema) values (?)" (str (schema conn))]
                                  (pg? c) (let [schema-val (str (schema conn))]
                                            ["insert into coast_schema (schema) values (?) on conflict (schema) do update set schema = ?" schema-val schema-val])
                                  :else []))))))))
