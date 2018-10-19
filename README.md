# lighthouse
_clojure + sqlite + hikaricp connection pool + datomic inspired queries_

## Quickstart

Add this to your `deps.edn`

```clojure
{:deps {coast-framework/lighthouse {:mvn/version "1.0.0"}}}
```

Get sqlite superpowers by adding it your project like this

```clojure
(ns your-project
  (:require [lighthouse.core :as db]))
```

Connect to your db like this:

```clojure
(def conn (db/connect "todos.db"))
; just include everything after jdbc:sqlite: in a normal connection string
```

Change your db schema like this

```clojure
(def todos-users [{:db/col :person/name :db/type "text" :db/unique? true :db/nil? false}
                  {:db/rel :person/todos :db/type :many :db/ref :todo/person}
                  {:db/rel :todo/person :db/type :one :db/ref :person/id}
                  {:db/col :todo/name :db/type "text"}
                  {:db/col :todo/done :db/type "boolean" :db/default false :db/nil? false}])

; you can run this over and over, lighthouse keeps track
; in a schema_migrations table in your db
(db/migrate conn todos-users)

; if you change the name of the def like todos-users -> todos-users-1
; then this will *not* be treated like a new migration
; if you change the contents of the def, then this will attempt to run a
; new migration

; if you're unsure of what's about to happen, run this instead
; it will output the migration sql instead
(sql/migrate todos-users)

; there is a version of each function (q, pull, insert, update!, upsert, delete)
; in the lighthouse.sql namespace if you're ever like what the heck is happening?
```

Made a mistake with a column name? No problem, rename columns like this

```clojure
(def rename-done [{:db/id :todo/done :db/col :todo/finished}])

(db/migrate conn rename-done)
```

Insert data into the database like this

```clojure
; simple one row insert
(let [p (db/insert conn {:person/name "swlkr"})

  ; insert multiple rows like this
  ; p is auto-resolved to (get p :person/id)
  (db/insert conn [{:todo/name "write readme"
                    :todo/person p
                    :todo/done true}
                   {:todo/name "write tests 😅"
                    :todo/person p
                    :todo/done false}]})

; or just manually set the foreign key integer value
(db/insert conn [{:todo/name "write readme"
                  :todo/person 1
                  :todo/done true}
                 {:todo/name "write tests 😅"
                  :todo/person 1
                  :todo/done false}]})
```

Update data like this

```clojure
(db/update conn {:todo/id 2 :todo/name "write tests 😅😅😅"})

; there is no update with a where clause, you'll have to select then multi update
(let [todos (db/q conn '[:select todo/id
                         :where [todo/done false]]) ; => [{:todo/id 2} {:todo/id 3}]
  (db/update conn (map #(assoc % :todo/done true) todos)))
```

Query data like this

```clojure
(db/q conn '[:select todo/name todo/done
             :where [todo/done ?done]]
             :order todo/created-at desc
           {:done true})
; => [{:todo/name "write readme" :todo/done true}]

; or like this
(db/q conn '[:select todo/name todo/done
             :where [todo/done true]])
; => [{:todo/name "write readme" :todo/done true}]

; or like this
(db/q conn '[:pull [person/name {:person/todos [todo/name todo/done]}]
             :where [todo/done ?todo/done]]
           {:todo/done true})
; => [{:person/name "swlkr" :person/todos [{:todo/name "write readme" :todo/done true}]}]

; or like this
(db/pull conn '[person/name {:person/todos [todo/name todo/done]}]
              [:person/name "swlkr"]])
; => {:person/name "swlkr" :person/todos [{:todo/name "write readme" :todo/done true}]}

; if you don't want to specify every column in great detail, you don't have to
(db/q conn '[:select todo/*
             :where [todo/done false]])
; => [{:todo/id 1 :todo/name ... :todo/done false :todo/created-at ...}]

; joins are supported too
(db/q conn '[:select todo/* person/*
             :joins person])
; => [{:todo/id 1 :todo/name ... :person/id 1 :person/name "swlkr" ...}]
```

Deleting data like this

```clojure
(db/delete conn {:todo/id 1})

; or multiple rows like this
(db/delete conn [{:todo/id 1} {:todo/id 2} {:todo/id 3}])

; delete only works for keys named "id" for now
```
