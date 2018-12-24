(ns lighthouse.ddl-test
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [lighthouse.ddl :refer [create-table text add-column drop-column drop-table
                                    create-extension drop-extension add-foreign-key
                                    decimal add-index add-reference
                                    alter-column timestamps]]
            [pjstadig.humane-test-output :as humane-test-output]))

(defn setup [f]
  (humane-test-output/activate!)
  (f))


(deftest sqlite-create-table-test
  (testing "create table with nothing"
    (is (= "create table customer ( id integer primary key )"
           (create-table :sqlite :customer))))

  (testing "create table in sqlite with timestamps"
    (is (= "create table customer ( id integer primary key, updated_at timestamp, created_at timestamp not null default current_timestamp )"
           (create-table :sqlite :customer
             (timestamps :sqlite))))))

(deftest pg-create-table-test
  (testing "create table in postgres with timestamp columns"
    (is (= "create table customer ( id serial primary key, updated_at timestamptz, created_at timestamptz not null default now() )"
           (create-table :pg :customer
             (timestamps :pg)))))

  (testing "create table in postgres with default columns"
    (is (= "create table customer ( id serial primary key )"
           (create-table :pg :customer)))))

(deftest text-test
  (testing "text with all args"
    (is (= "email text unique collate nocase not null default ''"
           (text :email :unique true :collate "nocase" :null false :default "''")))))

(deftest decimal-test
  (testing "decimal with null, default, scale and precision args"
    (is (= "amount decimal(15,2) not null default 0"
           (decimal :amount :null false :default 0 :precision 15 :scale 2))))

  (testing "decimal with not null, default, no scale or precision args"
    (is (= "amount decimal not null default 0"
           (decimal :amount :null false :default 0))))

  (testing "decimal with not null, default, only scale arg"
    (is (= "amount decimal(0,2) not null default 0"
           (decimal :amount :null false :default 0 :scale 2))))

  (testing "decimal with not null, default, only precision arg"
    (is (= "amount decimal(15) not null default 0"
           (decimal :amount :null false :default 0 :precision 15)))))

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

(deftest add-foreign-key-test
  (testing "all options"
    (is (= "alter table todo add constraint fk_name foreign key (customer_id) references customer (user_id) on delete cascade on update nullify")
        (add-foreign-key :todo :customer
                         :col :customer-id
                         :pk :user-id
                         :name "fk-name"
                         :on-delete :cascade
                         :on-update :nullify))))

(deftest add-index-test
  (testing "one column index"
    (is (= "create index customer_email_index on customer (email)"
           (add-index :customer :email))))

  (testing "unique two column index"
    (is (= "create unique index customer_email_name_index on customer (email, name)"
           (add-index :customer [:email :name] :unique true))))

  (testing "unique two column index with custom name"
    (is (= "create unique index custom_index on customer (email, name)"
           (add-index :customer [:email :name] :unique true :name "custom_index"))))

  (testing "one column index with order"
    (is (= "create index customer_email_desc_index on customer (email desc)"
           (add-index :customer :email :order {:email :desc}))))

  (testing "one column index with order and where"
    (is (= "create index customer_email_desc_index on customer (email desc) where gmail"
           (add-index :customer :email :order {:email :desc} :where "gmail"))))

  (testing "unique two column index with order and where"
    (is (= "create unique index customer_email_desc_name_asc_index on customer (email desc, name asc) where active")
        (add-index :customer [:email :name] :unique true :where "active" :order {:email :desc :name :asc}))))

(deftest add-reference-test
  (testing "basic reference"
    (is (= "alter table todo add column customer integer references customer (id)"
           (add-reference :todo :customer)))))

(deftest alter-column-test
  (testing "basic alter column with using"
    (is (= "alter table todo alter column customer type datetime using now()"
           (alter-column :todo :customer :datetime :using "now()")))))
