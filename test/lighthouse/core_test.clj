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
  (f)
  (delete conn {:person/id 1})
  (delete conn {:todo/id 1}))

(use-fixtures :once setup-db)

(deftest insert-test
  (testing "insert with sql without rel"
    (is (= '(1) (transact conn '[:insert person/name
                                 :values ["sean"]]))))

  (testing "insert related data"
    (is (= '(1) (insert conn {:todo/person 1 :todo/name "todo #1" :todo/done false})))))

(deftest join-test
  (testing "basic join"
    (is (= [{:person/name "sean" :todo/name "todo #1"}] (q conn '[:select person/name todo/name
                                                                  :from person
                                                                  :joins todo/person])))))
