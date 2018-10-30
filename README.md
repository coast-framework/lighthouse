# lighthouse
_clojure + sqlite + hikaricp connection pool + datomic inspired queries_

## Installation

Add this to your `deps.edn`

```clojure
coast-framework/lighthouse {:mvn/version "0.3.0"}
```

## Require

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

## Migrations

Change your db schema like this

```clojure
(def todos-users [{:db/col :person/name :db/type "text" :db/unique? true :db/nil? false}
                  {:db/rel :person/todos :db/type :many :db/ref :todo/person}
                  {:db/rel :todo/person :db/type :one :db/ref :person/id}
                  {:db/col :todo/name :db/type "text"}
                  {:db/col :todo/done :db/type "boolean" :db/default false :db/nil? false}])
```

You can run this over and over, lighthouse keeps track in a schema_migrations table in your db

```clojure
(db/migrate conn todos-users)
```

If you change the name of the def like todos-users -> todos-users-1
then this will *not* be treated like a new migration
if you change the contents of the def, then this will attempt to run a
new migration. If you're unsure of what's about to happen, run this instead it will output the sql and do nothing to the database

```clojure
(sql/migrate todos-users)
```

Made a mistake with a column name? No problem, rename columns like this

```clojure
(def rename-done [{:db/id :todo/done :db/col :todo/finished}])

(db/migrate conn rename-done)
```

### Tables

All tables are created from a vector of maps, so this

```clojure
[{:db/col :person/nick :db/type "text" :db/unique? true :db/nil? false :db/default "''"}
 {:db/col :todo/name :db/type "text"}]
```

turns into this:

```sql
create table if not exists person (
  id integer primary key,
  updated_at timestamp,
  created_at timestamp not null default current_timestamp
)

create table if not exists todo (
  id integer primary key,
  updated_at timestamp,
  created_at timestamp not null default current_timestamp
)
```

The namespaces of the `:db/col` value in this case `:person/nick` -> `person` and `:todo/name` -> `todo` are the table names. This happens for the distinct namespaces of every key in every migration

### Columns

Columns are the names of the `:db/col` values in the migration maps, so this:

```clojure
[{:db/col :person/nick :db/type "text" :db/unique? true :db/nil? false :db/default "''"}
 {:db/col :todo/name :db/type "text"}]
```

Becomes this:

```sql
alter table person add column nick text not null default '';
create unique index idx_person_nick on table person (nick);

alter table todo add column name text;
```

SQLite has several restrictions on what alter table can and can't do, namely, altering a table to add a column must have a default value when specifying not null, this will fail if you don't specify something.

### Relationships (Foreign Keys)

Foreign keys are a little special and require two, yes that's right, two maps to function as keys in pull queries. For example:

```clojure
[{:db/rel :person/todos :db/type :many :ref :todo/person}
 {:db/rel :todo/person :db/type :one :ref :person/id}]
```

Turns into one alter table statement with a references clause:

```sql
alter table todo add column person integer references person(id) on cascade delete;
```

You can control the `on cascade` behavior with the optional `:db/delete` key.

## Data

### Insert

Insert data into the database like this

```clojure
; simple one row insert
(let [p (db/insert conn {:person/name "swlkr"})]

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
                  :todo/done false}]}))
```

### Update

Update data like this

```clojure
(db/update conn {:todo/id 2 :todo/name "write tests 😅😅😅"})

; you can either perform an update after a select
(let [todos (db/q conn '[:select todo/id
                         :where [todo/done false]])] ; => [{:todo/id 2} {:todo/id 3}]
  (db/update conn (map #(assoc % :todo/done true) todos)))

; or update from transact
(db/transact conn '[:update todo
                    :set [todo/done true]
                    :where [todo/id [2 3]]])
```

### Queries

Query data like this

```clojure
(db/q conn '[:select todo/name todo/done
             :where [todo/done ?done]]
             :order todo/created-at desc
           {:done true})
; => [{:todo/name "write readme" :todo/done true}]

; or like this
(db/q conn '[:select todo/name todo/done
             :from todo
             :where [todo/done true]])
; => [{:todo/name "write readme" :todo/done true}]

; if you don't want to specify every column, you don't have to
(db/q conn '[:select todo/*
             :from todo
             :where [todo/done false]])
; => [{:todo/id 1 :todo/name ... :todo/done false :todo/created-at ...}]

; joins are supported too
(db/q conn '[:select todo/* person/*
             :from todo
             :joins person])
; => [{:todo/id 1 :todo/name ... :person/id 1 :person/name "swlkr" ...}]
```

### Pull Queries

Pull queries are special because they don't map to sql 1:1, they return your data hierarchically with the help of some left outer join json aggregation and `clojure.walk` after the fact. They work together with the schema migrations to make your life easier, or that's the goal anyway.

Given this migration:

```clojure
(def todos-users [{:db/col :person/name :db/type "text" :db/unique? true :db/nil? false}
                  {:db/rel :person/todos :db/type :many :db/ref :todo/person}
                  {:db/rel :todo/person :db/type :one :db/ref :person/id}
                  {:db/col :todo/name :db/type "text"}
                  {:db/col :todo/done :db/type "boolean" :db/default false :db/nil? false}])

(db/migrate conn todos-users)
```

You can run these queries

```clojure
(db/q conn '[:pull [person/name {:person/todos [todo/name todo/done]}]
             :from person
             :where [todo/done ?todo/done]]
           {:todo/done true})
; => [{:person/name "swlkr" :person/todos [{:todo/name "write readme" :todo/done true}]}]

; or like this
(db/pull conn '[person/name {:person/todos [todo/name todo/done]}]
              [:person/name "swlkr"]])
; => {:person/name "swlkr" :person/todos [{:todo/name "write readme" :todo/done true}]}

; or you can go the other way as well
(db/pull conn '[todo/name {:person/todo [todo/name todo/done]}])
              [:todo/name 'like "%blog post"]
```

### Delete

Delete data like this

```clojure
(db/delete conn {:todo/id 1})

; or multiple rows like this
(db/delete conn [{:todo/id 1} {:todo/id 2} {:todo/id 3}])

; there's always transact too
(db/transact conn '[:delete
                    :from todo
                    :where [todo/id [1 2 3]]]) ; this implicitly does an in query
```
