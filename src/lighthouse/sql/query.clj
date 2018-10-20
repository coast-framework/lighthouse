(ns lighthouse.sql.query
  (:require [clojure.string :as string]
            [clojure.walk :as walk]
            [lighthouse.util :refer [snake-case sql-vec? qualified-col-name]]))

(defn op? [k]
  (contains? #{:select :from :update :set :insert :insert-into :delete :pull :joins :join :where :order :limit :offset :group :group-by} k))

(defn sql-map [v]
  (let [parts (partition-by op? v)
        ops (take-nth 2 parts)
        args (filter #(not (contains? (set ops) %)) parts)]
    (zipmap (map first ops) (map vec args))))

(defn replace-val [q params val]
  (if (string/starts-with? (str val) "?")
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
  (let [t (-> k namespace keyword)]
    (get schema t)))

(defn select [schema v]
  (let [v (->> (map #(expand-star schema %) v)
               (mapcat identity))
        s (->> (map select-col v)
               (string/join ", "))]
    (if (not (string/blank? s))
      {:select (str "select " s)
       :select-ks v}
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
  (let [kw-args (map keyword args)]
    {:joins (->> (select-keys schema kw-args)
                 (map (fn [[_ v]] [(:db/ref v) (:db/rel v)]))
                 (map join)
                 (string/join "\n"))
     :join-ks (->> (select-keys schema kw-args)
                   (map (fn [[_ v]] (:db/ref v))))}))

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

(defn flat [coll]
  (mapcat #(if (sequential? %) % [%]) coll))

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
  {:delete (str "delete")})

(defn sql-part [schema [k v]]
  (condp = k
    :select (select schema v)
    :from (from v)
    ; :pull (pull v)
    :joins (joins schema v)
    :join (joins schema v)
    :where (where v)
    :order (order v)
    :limit (limit v)
    :offset (offset v)
    :group (group v)
    :group-by (group v)
    :delete (delete v)
    ;:insert (insert v)
    ;:insert-into (insert v)
    ;:update (update v)
    nil))

(defn sql-vec [schema v params]
  (->> (replace-vals v params)
       (sql-map)
       (map #(sql-part schema %))))
