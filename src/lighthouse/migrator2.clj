(ns lighthouse.migrator2)

(defn gen
  "Creates a new timestamped clj migration file in resources/migrations"
  [s])

(defn from-resources
  "Reads migration filenames from resources/migrations"
  [])

(defn from-db
  "Reads migration filenames from the db schema_migrations table"
  [])

(defn pending
  "Compares resource/migrations filenames and schema_migrations table filenames"
  [resources db])

(defn migrate
  "Migrates a database schema"
  [conn migrations])

(defn rollback
  "Rolls back a database schema"
  [migration])
