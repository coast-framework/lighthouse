(ns lighthouse.core-test
  (:require [lighthouse.core :as db]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [pjstadig.humane-test-output :as humane-test-output]
            [clojure.java.io :as io]))

(def conn (db/connect "jdbc:sqlite:test.db"))

(defn setup [f]
  (let [todos-users [{:db/col :person/name :db/type "text" :db/unique? true}
                     {:db/col :item/name :db/type "text" :db/unique? true}
                     {:db/rel :item/todos :db/type :many :db/ref :todo/item}
                     {:db/rel :todo/item :db/type :one :db/ref :item/id}
                     {:db/rel :person/todos :db/type :many :db/ref :todo/person}
                     {:db/rel :todo/person :db/type :one :db/ref :person/id}
                     {:db/col :todo/name :db/type "text"}
                     {:db/col :todo/done :db/type "boolean" :db/default false :db/nil? false}]]
    (humane-test-output/activate!)
    (db/migrate conn todos-users)
    (f)
    (io/delete-file "test.db")))

(use-fixtures :once setup)

(deftest sql-test
  (testing "select without from"
    (is (= ["select todo.id as todo$id from todo"]
           (db/sql conn '[:select todo/id]))))

  (testing "join without from"
    (is (= ["select todo.created_at as todo$created_at, todo.done as todo$done, todo.id as todo$id, todo.item as todo$item, todo.name as todo$name, todo.person as todo$person, todo.updated_at as todo$updated_at, person.created_at as person$created_at, person.id as person$id, person.name as person$name, person.updated_at as person$updated_at from person join todo on todo.person = person.id"]
           (db/sql conn '[:select todo/* person/*
                          :joins person/todos])))))

(db/defq conn "test.sql")

(deftest defq-test
  (testing "defq with a sql file"
    (is (= '()
           (all-todos)))))
