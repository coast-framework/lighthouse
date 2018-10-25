(ns lighthouse.defq
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.java.jdbc :as jdbc]
            [lighthouse.util :refer [map-keys snake-case kebab-case flat]])
  (:import [java.io FileNotFoundException]))

(def name-regex #"^--\s*name\s*:\s*(.+)$")
(def fn-regex #"^--\s*fn\s*:\s*(.+)$")
(def qualified-keyword-regex #"[^:]:([\w-\.]+/?[\w-\.]+)Z?")

(defn name-line? [s]
  (not (nil? (re-matches name-regex s))))

(defn fn-line? [s]
  (not (nil? (re-matches fn-regex s))))

(defn sql-line? [s]
  (nil? (re-matches #"^--.*$" s)))

(defn parse-name [s]
  (when s
    (let [[_ name] (re-matches name-regex s)]
      name)))

(defn parse-fn [s]
  (let [[_ f] (re-matches fn-regex (or s ""))]
    (if (nil? f)
      (resolve (symbol "identity"))
      (resolve (symbol f)))))

(defn parse-query-string [query-string]
  (let [query-lines (clojure.string/split query-string #"\n")
        name (-> (filter name-line? query-lines)
                 (first)
                 (parse-name))
        f-name (->> (filter fn-line? query-lines)
                    (first))
        f (parse-fn f-name)
        sql (clojure.string/join " " (filter sql-line? query-lines))]
    (if (nil? name)
      nil
      {:sql sql :f f :name name :fn f-name})))

(defn parse [lines]
  (let [query-lines (clojure.string/split lines #"\n\n")]
    (filter #(not (nil? %)) (map parse-query-string query-lines))))

(defn parameterize [s m]
  (string/replace s qualified-keyword-regex (fn [[_ arg]]
                                              (let [k (keyword arg)
                                                    v (get m k)]
                                                (if (coll? v)
                                                 (->> (map (fn [_] (str "?")) v)
                                                      (string/join ",")
                                                      (str (first _))) ; dirty hack for ::
                                                 (str (first _) "?")))))) ; same

(defn has-keys? [m keys]
  (every? #(contains? m %) keys))

(defn sql-ks [sql]
  (->> (re-seq qualified-keyword-regex sql)
       (map second)
       (map string/trim)
       (map keyword)
       (vec)))

(defn sql-tuples [sql m]
  (let [m (map-keys snake-case m)]
    (->> (sql-ks sql)
         (map (fn [k] [k (get m k)])))))

(defn sql-vec [sql m]
  (when (string? sql)
    (let [m (->> (or m {})
                 (map-keys snake-case))
          tuples (sql-tuples sql m)
          params (reduce conj {} tuples)
          p-sql (parameterize sql params)
          values (mapv second tuples)
          ks (map first tuples)
          v (flat (apply conj [p-sql] values))]
      (if (has-keys? m ks)
        v
        (->> (set/difference (set ks) (set (keys m)))
             (string/join ", ")
             (format "Missing keys: %s")
             (Exception.)
             (throw))))))

(defn slurp-resource [filename]
  (or (some-> filename io/resource slurp)
      (throw (FileNotFoundException. filename))))

(defn query [name filename]
  (->> (slurp-resource filename)
       (parse)
       (filter #(= (:name %) name))
       (first)))

(defn create-root-var [name value]
  (intern *ns*
          (with-meta (symbol name)
                     (meta value))
          value))

(defn query-fn [conn {:keys [sql f]}]
  (fn [& [m]]
    (let [v (sql-vec sql m)
          rows (jdbc/query conn v {:keywordize? true :identifiers kebab-case})]
      (f rows))))

(defn query-fns [conn filename]
   (doall (->> (slurp-resource filename)
               (parse)
               (map #(assoc % :ns *ns*))
               (map #(create-root-var (:name %) (query-fn conn %))))))
