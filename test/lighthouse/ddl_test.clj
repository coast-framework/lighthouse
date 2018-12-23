(ns lighthouse.ddl-test
  (:require [clojure.test :refer [deftest testing is]]
            [lighthouse.ddl :refer [create-table text add-column drop-column drop-table
                                    create-extension drop-extension]]))

(deftest sqlite-create-table-test
  (testing "create table in sqlite with default columns"
    (is (= "create table customer ( id integer primary key, updated_at timestamp, created_at timestamp not null default current_timestamp )"
           (create-table :sqlite :customer)))))

(deftest pg-create-table-test
  (testing "create table in sqlite with default columns"
    (is (= "create table customer ( id serial primary key, updated_at timestamptz, created_at timestamptz not null default now() )"
           (create-table :pg :customer)))))

(deftest text-test
  (testing "text with all args"
    (is (= "email text unique collate nocase not null default ''"
           (text :email :unique true :collate "nocase" :null false :default "''")))))

(deftest drop-table-test
  (testing "drop table"
    (is (= "drop table customer"
           (drop-table :customer)))))

(deftest drop-column-test
  (testing "drop column"
    (is (= "alter table customer drop column email"
           (drop-column :customer :email)))))

(deftest add-column-test
  (testing "add column"
    (is (= "alter table customer add column email text"
           (add-column :customer :email :text)))))

(deftest create-extension-test
  (testing "create extension"
    (is (= "create extension citext"
           (create-extension "citext")))))

(deftest drop-extension-test
  (testing "drop extension"
    (is (= "drop extension citext"
           (drop-extension "citext")))))
