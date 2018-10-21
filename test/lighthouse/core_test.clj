(ns lighthouse.core-test
  (:require [clojure.test :refer [deftest testing is]]
            [lighthouse.core :refer [connect migrate transact]]))

(def conn (connect "test.db"))

(deftest connect-test
  (testing "connect works"
    (is (and (map? conn)
             (contains? conn :datasource)))))

(deftest migrate-test
  (testing "migrate works"
    (migrate conn [{:db/col :person/name :db/type "text" :db/unique? true :db/nil? false :db/default "''"}]))

  (testing "rel migrations work"
    (migrate conn [{:db/rel :person/todos :db/type :many :db/ref :todo/person}
                   {:db/rel :todo/person :db/type :one :db/ref :person/id}
                   {:db/col :todo/name :db/type "text"}
                   {:db/col :todo/done :db/type "boolean"}])))

(deftest insert-test
  (testing "insert with sql without rel"
    (transact '[:insert person/name
                :values "sean"])))
