(ns lighthouse.core-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.java.io :as io]
            [lighthouse.core :refer [connect migrate q transact insert delete]]))

(def conn (connect "test.db"))

(defn setup-db [f]
  (migrate conn [{:db/col :person/name :db/type "text" :db/unique? true :db/nil? false :db/default "''"}
                 {:db/rel :person/todos :db/type :many :db/ref :todo/person}
                 {:db/rel :todo/person :db/type :one :db/ref :person/id}
                 {:db/col :todo/name :db/type "text"}
                 {:db/col :todo/done :db/type "boolean" :db/nil? false :db/default false}])
  (delete conn {:person/name "sean"})
  (f))

(use-fixtures :once setup-db)

(deftest insert-test)
  ; (testing "insert with sql without rel"
  ;   (transact conn '[:insert person/name
  ;                    :values ["sean"]]))
  ;
  ; (testing "insert related data"
  ;   (insert conn {:todo/person 1 :todo/name "todo #1" :todo/done false})))

(deftest join-test
  (testing "basic join"
    (q conn '[:select person/name todo/name
              :from person
              :joins todo/person])))
