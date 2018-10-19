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
(lighthouse.sql/migrate todos-users)

; there is a version of each function (q, pull, insert, update!, upsert, delete)
; in the sql namespace if you're ever like what the heck is happening?
; also, PR's welcome 😎!
```

Made a mistake with a column name? No problem, rename columns like this

```clojure
(def rename-done [{:db/id :todo/done :db/col :todo/finished}])

(db/migrate conn rename-done)
```

Need to drop a column?

```clojure
(def better-done [{:db/id :todo/done :db/col nil} ; set :db/col to nil to drop
                  {:db/col :todo/done-at :db/type "timestamp" :db/default "now()" :db/nil? false :db/index? true}]) ; create a new column done_at

(db/migrate conn rename-done)
```

Insert data into the database like this

```clojure
(db/insert conn {:person/name "swlkr"
                 :person/todos [{:todo/name "write readme"
                                 :todo/done true}

                                {:todo/name "write tests 😅"
                                 :todo/done false}]})

; returns

{:person/name "swlkr"
 :person/todos [{:todo/name "write readme"
                 :todo/done true
                 :todo/id 1}
                {:todo/name "write tests 😅"
                 :todo/done false
                 :todo/id 2}]}

; or like this

(db/insert conn {:todo/name "write a blog post"
                 :todo/done false})

; or insert multiple rows like this

(db/insert conn [{:person/name "swlkr1"} {:person/name "swlkr2"}])
```

Update data like this

```clojure
; don't forget the ! didn't want to overwrite a core clojure function
(db/update! conn {:todo/id 2 :todo/name "write tests 😅😅😅"})

; there's also this
(db/upsert conn {:todo/id 2 :todo/name "write tests 😅😅😅"})
```

Query data like this

```clojure
(db/q conn '[:select todo/name todo/done
             :where [todo/done ?todo/done]]
           {:todo/done true})
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
              [person/name "swlkr"]])
; => {:person/name "swlkr" :person/todos [{:todo/name "write readme" :todo/done true}]}
```

Delete data like this

```clojure
(db/delete conn {:todo/id 1})

; or multiple rows like this

(db/delete conn [{:todo/id 1} {:todo/id 2} {:todo/id 3}])
```
