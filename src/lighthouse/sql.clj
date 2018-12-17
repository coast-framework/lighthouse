(ns lighthouse.sql
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [lighthouse.util :refer [table snake-case sql-vec? qualified-col-name rel? flat namespace* name*]])
  (:refer-clojure :exclude [update])
  (:import (java.time LocalDateTime)))

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

(defn expand-star [schema v]
  (if (contains? (set (map name v)) "*")
    (->> (map namespace v)
         (distinct)
         (map keyword)
         (mapcat #(get schema %))
         (concat v)
         (filter #(not= "*" (name %))))
    v))

(defn select [schema v]
  (let [v (expand-star schema v)
        s (->> (map select-col v)
               (string/join ", "))]
    (if (not (string/blank? s))
      {:select (str "select " s)
       :select-ks v}
      (throw (Exception. (str "select needs at least one argument. You typed :select"))))))

(defn rel-opts [m]
  (let [k (-> m keys first)]
    (if (sequential? k)
      (sql-map (drop 1 k))
      {})))

(defn pull-limit [[i]]
  (when (pos-int? i)
    {:limit (str "where rn <= " i)}))

(defn order [v]
  {:order (str "order by " (->> (partition-all 2 v)
                                (mapv vec)
                                (mapv #(if (= 1 (count %))
                                         (conj % 'asc)
                                         %))
                                (mapv #(str (qualified-col-name (first %)) " " (name (second %))))
                                (string/join ", ")))})

(defn pull-sql-part [[k v]]
  (condp = k
    :order (order v)
    :limit (pull-limit v)
    :else {}))

(defn pull-from [o table]
  (let [order (or o "order by id")]
    (string/join "\n" ["("
                       "select"
                       (str "  " table ".*, ")
                       (str "   row_number() over (" order ") as rn")
                       (str "from " table)
                       (str ") as " table)])))

(defn one-join-col [k]
  (str (-> k name snake-case) ".id"))

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

(defn rel-key [m]
  (let [k (-> m keys first)]
    (if (sequential? k)
      (-> k first)
      k)))

(defn pull-col [k]
  (str (-> k namespace snake-case) "$" (-> k name snake-case)))

(defn json-build-object [k]
  (str "'" (pull-col k) "', " (qualified-col-name k)))

(defn rel-col [k]
  (if (qualified-ident? k)
    (str "'" (pull-col k) "', " (pull-col k))
    (str "'" (-> k first pull-col) "', " (-> k first pull-col))))

(def pull-sql-map {:sqlite {:json-agg "json_group_array"
                            :json-object "json_object"}
                   :pg {:json-agg "json_agg"
                        :json-object "json_build_object"}})

(defn pull-join [db schema m]
  (let [k (rel-key m)
        left (get-in schema [k :db/ref])
        right (or (get-in schema [left :db/ref]) k)
        joins left
        {:keys [order limit]} (->> (rel-opts m)
                                   (map pull-sql-part)
                                   (apply merge))
        val (-> m vals first)
        v (filter qualified-ident? val)
        maps (filter map? val)
        child-cols (map #(-> % keys first rel-col) maps)]
    (->> ["left outer join ("
          "select"
          (str (if (nil? joins)
                 (one-join-col (keyword k))
                 (join-col joins)) ",")
          (str (-> pull-sql-map db :json-agg)
               "("
              (-> pull-sql-map db :json-object)
              "(")
          (->> (expand-star schema v)
               (map json-build-object)
               (concat child-cols)
               (string/join ","))
          (str ")) as " (pull-col k))
          (str "from " (if (nil? joins)
                         (->> k keyword name snake-case (pull-from order))
                         (->> joins namespace snake-case (pull-from order))))
          (->> (map #(pull-join db schema %) maps)
               (string/join "\n"))
          limit
          (str "group by " (if (nil? joins)
                             (one-join-col (keyword k))
                             (join-col joins)))
          (str ") " (if (nil? joins)
                      (one-join-statement (keyword k))
                      (join-statement [left right])))]
         (filter some?)
         (string/join "\n"))))

(defn pull-joins [db schema acc v]
  (let [maps (filter map? v)
        joins (map #(pull-join db schema %) maps)
        acc (concat acc joins)]
    (if (empty? maps)
     acc
     (pull-joins db schema acc (map #(-> % vals first) maps)))))

(defn pull [db schema [v]]
  (let [cols (->> (filter qualified-ident? v)
                  (expand-star schema))
        maps (filter map? v)
        rel-cols (->> (map rel-key maps)
                      (map pull-col))
        col-sql (string/join ", " (concat (map select-col cols)
                                          rel-cols))
        joins (pull-joins db schema [] v)]
    {:select (str "select " col-sql)
     :from (str "from " (or (-> cols first namespace* snake-case)
                            (-> (map rel-key maps) first namespace* snake-case)))
     :joins (string/join "\n" joins)}))

(defn from [v]
  {:from (str "from " (string/join " " v))})

(defn from-clause [s-ks j-ks]
  (when (or (not (empty? s-ks))
            (not (empty? j-ks)))
    (let [t (-> (map #(-> % namespace snake-case) s-ks) (first))
          j (-> (map #(-> % name snake-case) j-ks) (first))]
      (str "from " (or j t)))))

(defn join [v]
  (str "join " (join-statement v)))

(defn get-one-rel [schema k]
  (or (get schema (-> schema k :db/ref))
      (get schema k)))

(defn joins [schema args]
  (let [args (map keyword args)
        one-rels (map #(get-one-rel schema %) args)]
    {:joins (->> one-rels
                 (map (fn [m] [(:db/rel m) (:db/ref m)]))
                 (map join)
                 (string/join "\n"))
     :join-ks (map :db/rel one-rels)}))

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
  (let [v (conj v [:updated-at (LocalDateTime/now)])
        args (filter #(not= "id" (-> % first name)) v)]
    {:update-set (str "set " (->> (map (fn [[k _]] (str (-> k name snake-case) " = ?")) args)
                                  (distinct)
                                  (string/join ", ")))
     :update-set-args (map second args)}))

(defn sql-part [db schema [k v]]
  (condp = k
    :select (select schema v)
    :from (from v)
    :pull (pull db schema v)
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

(defn sql-vec [db schema v params]
  (let [m (->> (replace-vals v params)
               (sql-map)
               (map #(sql-part db schema %))
               (apply merge))
        {:keys [select select-ks pull joins join-ks where order offset limit group args delete insert values update update-set update-set-args]} m
        from-clause (or (:from m) (from-clause select-ks join-ks))
        sql (->> (filter some? [select pull delete update update-set insert values from-clause joins where order offset limit group])
                 (string/join " "))]
    (apply conj [sql] (concat update-set-args (filter some? args)))))
