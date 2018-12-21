(ns lighthouse.sql-test
  (:require [lighthouse.sql :as sql]
            [clojure.test :refer [deftest testing is use-fixtures]]
            [pjstadig.humane-test-output :as humane-test-output]))

(defn setup [f]
  (humane-test-output/activate!)
  (f))

(use-fixtures :once setup)

(deftest where-test
  (testing "where with and, not"
    (is (= (sql/where '[and [member/name != ""]
                            [member/id 1]])
           {:where "where (member.name != ? and member.id = ?)" :args '("" 1)})))

  (testing "where not and implicit and"
    (is (= (sql/where '[[member/name != ""]
                        [member/id ""]])
           {:where "where (member.name != ? and member.id = ?)" :args '("" "")})))

  (testing "where or"
    (is (= (sql/where '[or [member/name ""]
                           [member/id 1]
                           [member/active != false]])
           {:where "where (member.name = ? or member.id = ? or member.active != ?)" :args '("" 1 false)})))

  (testing "where and & or"
    (is (= (sql/where '[or [member/name "test"]
                           [member/id 1]
                        and [member/id 2]
                            [member/active != false]])
           {:where "where (member.name = ? or member.id = ?) and (member.id = ? and member.active != ?)"
            :args '("test" 1 2 false)})))

  (testing "where string"
    (is (= (sql/where ["member.created_at < ?" "2018-11-24"])
           {:where "where member.created_at < ?"
            :args '("2018-11-24")}))))

(deftest sql-vec
  (testing "sql-vec with select, limit and where clause"
    (is (= ["select member.name as member$name, member.email as member$email from member where (member.name = ? and member.email = ?) limit 1" "test" "test@test.com"]
           (sql/sql-vec :pg {} '[:select member/name member/email
                                 :where [member/name "test"]
                                        [member/email "test@test.com"]
                                 :limit 1]
                               {}))))

  (testing "sql-vec with an or where clause"
    (is (= (sql/sql-vec :pg {} '[:select member/name member/email
                                 :where or [member/name "test"]
                                           [member/email "test@test.com"]
                                        and [member/name != nil]
                                            [member/id != 1]
                                 :limit 1]
                               {})
           ["select member.name as member$name, member.email as member$email from member where (member.name = ? or member.email = ?) and (member.name is not null and member.id != ?) limit 1" "test" "test@test.com" 1])))

  (testing "a sql-vec that tries out most of the stuff"
    (is (= ["select member.id as member$id, member.name as member$name, member.email as member$email, todo.name as todo$name, todo.id as todo$id from member where (member.id != ? and todo.name != ?) order by todo.name desc, member.name asc offset 10 limit 1" 1 "hello"]
           (sql/sql-vec :pg {} '[:select member/id member/name member/email todo/name todo/id
                                 :where and [member/id != 1]
                                            [todo/name != "hello"]
                                 :limit 1
                                 :offset 10
                                 :order todo/name desc member/name]
                               {}))))

  (testing "a join with a select statement that doesn't include the main table"
    (let [schema {:member/todos {:db/rel :member/todos :db/ref :todo/member :db/type :many}
                  :todo/member {:db/rel :todo/member :db/type :one :db/ref :member/id}}]
      (is (= ["select todo.name as todo$name from member join todo on todo.member = member.id where (todo.name is not null)"]
             (sql/sql-vec :pg schema '[:select todo/name
                                       :joins member/todos
                                       :where [todo/name != nil]]
                                     {})))))

  (testing "variable parameters"
    (let [ids [1 2 3]]
      (is (= ["select todo.name as todo$name from todo where (todo.id in (?, ?, ?))" 1 2 3]
             (sql/sql-vec :pg {} '[:select todo/name
                                   :where [todo/id ?ids]]
                                 {:ids ids}))))))

(deftest from
  (testing "from with a select without the main table"
    (is (= "from member"
           (sql/from-clause [:todo/name] [:todo/member])))))
