(ns lighthouse.sql
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [lighthouse.util :refer [table snake-case sql-vec? qualified-col-name rel? flat]])
  (:refer-clojure :exclude [update])
  (:import (java.time Instant)))

(def ops #{:select :from :update :set :insert :delete :pull :joins :where :order :limit :offset :group :values})

(defn op? [k]
  (contains? ops k))

(defn sql-map [v]
  (let [parts (partition-by op? v)
        ops (take-nth 2 parts)
        args (filter #(not (contains? (set ops) %)) parts)]
    (zipmap (map first ops) (map vec args))))

(defn replace-val [q params val]
  (if (and (ident? val)
           (string/starts-with? (str val) "?"))
    (let [rk (-> (string/replace-first val #"\?" "")
                 (keyword))
          rv (get params rk)]
      (if (not (contains? params rk))
        (throw (Exception. (str "Parameter " val " is missing in query " q)))
        rv))
    val))

(defn replace-vals [v params]
  (walk/prewalk (partial replace-val v params) v))

(defn select-col [k]
  (str (qualified-col-name k)
       " as "
       (-> k namespace snake-case) "$" (-> k name snake-case)))

(defn expand-star [schema k]
  (get schema k))

(defn select [schema v]
  (let [v (if (contains? (set (map name v)) "*")
            (->> (map namespace v)
                 (distinct)
                 (map keyword)
                 (mapcat #(expand-star schema %))
                 (concat v)
                 (filter #(not= "*" (name %))))
            v)
        s (->> (map select-col v)
               (string/join ", "))]
    (if (not (string/blank? s))
      {:select (str "select " s)}
      (throw (Exception. (str "select needs at least one argument. You typed :select"))))))

(defn from [v]
  {:from (str "from " (string/join " " v))})

(defn join-col [k]
  (let [namespace (-> k namespace snake-case)
        name (-> k name snake-case)]
    (str namespace "." name)))

(defn one-join-statement [k]
  (str (-> k name snake-case)
       " on "
       (str (-> k name snake-case) ".id")
       " = "
       (join-col k)))

(defn join-statement [[left right]]
  (str (-> left namespace snake-case)
       " on "
       (qualified-col-name left)
       " = "
       (str (qualified-col-name right))))

(defn join [k]
  (str "join " (join-statement k)))

(defn joins [schema args]
  {:joins (->> (select-keys schema (map keyword args))
               (map (fn [[_ v]] [(:db/ref v) (:db/rel v)]))
               (map join)
               (string/join "\n"))})

(defn wrap-str [ws s]
  (if (string/blank? s)
    ""
    (str (first ws) s (second ws))))

(defn ? [val]
  (cond
    (coll? val) (->> (map (fn [_] "?") val)
                     (string/join ", ")
                     (wrap-str "()"))
    (nil? val) "null"
    :else "?"))

(defn not-op [val]
  (cond
    (sequential? val) "not"
    (nil? val) "is not"
    :else "!="))

(defn op [val]
  (cond
    (sequential? val) "in"
    (nil? val) "is"
    (= 'like val) "like"
    (= :like val) "like"
    :else "="))

(defn where-part [v]
  (if (vector? v)
    (let [[k op* val] v
          parts (if (= '!= op*)
                  [(qualified-col-name k) (not-op val) (? val)]
                  [(qualified-col-name k) (op op*) (? op*)])]
      (string/join " " parts))
    (throw (Exception. (str "where requires vectors to work. You typed: " v)))))

(defn where-clause [[k v]]
  (string/join (str " " (name k) " ")
               (map where-part v)))

(defn where-op? [k]
  (contains? '#{and or} k))

(defn where-vec [v]
  (let [v (if (vector? (first v))
            (into '[and] v)
            v)
        parts (partition-by where-op? v)
        ops (take-nth 2 parts)
        args (filter #(not (contains? (set ops) %)) parts)]
    (map vector (map first ops) (map vec args))))

(defn where-clauses [v]
  (let [wv (where-vec v)]
    (if (empty? wv)
      (throw (Exception. (str "where only accepts and & or. You typed: " (if (nil? v) "nil" v))))
      (map where-clause wv))))

(defn where [v]
  (if (sql-vec? v)
    {:where (str "where " (first v))
     :args (rest v)}
    {:where (str "where " (string/join " and " (map #(wrap-str "()" %) (where-clauses v))))
     :args (->> (filter vector? v)
                (mapv last)
                (filter some?)
                (flat))}))

(defn order [v]
  {:order (str "order by " (->> (partition-all 2 v)
                                (mapv vec)
                                (mapv #(if (= 1 (count %))
                                         (conj % 'asc)
                                         %))
                                (mapv #(str (qualified-col-name (first %)) " " (name (second %))))
                                (string/join ", ")))})

(defn limit [[i]]
  (if (pos-int? i)
    {:limit (str "limit " i)}
    (throw (Exception. (str "limit needs a positive integer. You typed: :limit " i)))))

(defn offset [[i]]
  (if (not (neg-int? i))
    {:offset (str "offset " i)}
    (throw (Exception. (str "offset needs a positive integer. You typed: :offset " i)))))

(defn group [v]
  {:group (str "group by " (->> (map qualified-col-name v)
                                (string/join ", ")))})

(defn delete [v]
  {:delete (str "delete")
   :from (str "from " (first v))})

(defn resolve-rel [schema val]
  (if (map? val)
    (as-> (vals schema) r
          (filter #(contains? (set (keys val)) (:db/ref %)) r)
          (first r)
          (:db/ref r)
          (get val r val))
    val))

(defn resolve-rels [schema val]
  (mapv #(resolve-rel schema %) val))

(defn insert [schema v]
  (let [table (-> v first namespace snake-case)]
    {:insert (str "insert into " table " ("
                  (->> (map name v)
                       (map snake-case)
                       (string/join ", "))
                 ")")}))

(defn values [schema v]
  (let [rv (mapv #(resolve-rels schema %) v)]
   {:values (str "values " (string/join ","
                            (map #(str "(" (->> (map (fn [_] "?") %)
                                                (string/join ","))
                                      ")")
                                 rv)))
    :args (flat rv)}))

(defn update [v]
  (let [table (-> v first snake-case)]
    {:update (str "update " table)}))

(defn update-set [schema v]
  (let [v (conj v [:updated-at (Instant/now)])]
    {:update-set (str "set " (->> (map (fn [[k _]] (str (-> k name snake-case) " = ?")) v)
                                  (string/join ", ")))
     :update-set-args (map second v)}))

(defn sql-part [schema [k v]]
  (condp = k
    :select (select schema v)
    :from (from v)
    ;:pull (pull v)
    :joins (joins schema v)
    :where (where v)
    :order (order v)
    :limit (limit v)
    :offset (offset v)
    :group (group v)
    :delete (delete v)
    :values (values schema v)
    :insert (insert schema v)
    :update (update v)
    :set (update-set schema v)
    nil))

(defn sql-vec [schema v params]
  (let [m (->> (replace-vals v params)
               (sql-map)
               (map #(sql-part schema %))
               (apply merge))
        {:keys [select pull from joins where order offset limit group args delete insert values update update-set update-set-args]} m
        sql (->> (filter some? [select pull delete update update-set insert values from joins where order offset limit group])
                 (string/join " "))]
    (apply conj [sql] (concat (filter some? update-set-args) (filter some? args)))))

(comment
  (def conn (lighthouse.core/connect "dev.db"))

  (sql-vec (lighthouse.core/schema conn) '[:insert todo/name todo/done todo/person
                                           :values ["test1" false {:person/id 1}]
                                                   ["test2" true {:person/id 1}]]
                                         {}))
