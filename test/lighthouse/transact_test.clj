(ns lighthouse.transact-test
  (:require [clojure.test :refer [is testing deftest]]
            [lighthouse.transact :as transact])
  (:refer-clojure :exclude [update]))

(deftest insert
  (testing "valid insert transaction"
    (is (= (transact/insert {:todo/name "hello"})
           '(:insert :todo/name
             :values ("hello"))))))

(deftest update
  (testing "valid update transaction"
    (is (= (transact/update {:todo/name "new todo" :todo/id 1})
           '(:update "todo" :where ["id" (1)] :set [:todo/name "new todo"] [:todo/id 1])))))

(deftest delete
  (testing "valid delete transaction"
    (is (= (transact/delete {:todo/id 1})
           '(:delete :from "todo" :where [:todo/id (1)])))))
