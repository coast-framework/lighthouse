(ns lighthouse.util
  (:require [clojure.string :as string]))

(defn convert-keyword [re replacement k]
  (if (keyword? k)
    (let [ns (-> (or (namespace k) "")
                 (string/replace re replacement))
          n (-> (or (name k) "")
                (string/replace re replacement))]
      (if (string/blank? ns)
        (keyword n)
        (keyword ns n)))
    k))

(defn convert-string [re replacement s]
  (if (string? s)
    (string/replace s re replacement)
    s))

(defn convert-case [re replacement val]
  (cond
    (keyword? val) (convert-keyword re replacement val)
    (string? val) (convert-string re replacement val)
    :else val))

(def kebab-case (partial convert-case #"_" "-"))
(def snake-case (partial convert-case #"-" "_"))

(defn col? [m]
  (contains? m :db/col))

(defn rel? [m]
  (contains? m :db/rel))

(defn qualified-col-name [k]
  (if (qualified-ident? k)
    (str (-> k namespace snake-case) "." (-> k name snake-case))
    (-> k name snake-case)))

(defn col-name [k]
  (-> k name snake-case))

(defn table [t]
  (str (->> (map first t)
            (filter qualified-ident?)
            (first)
            (namespace))))

(defn sql-vec? [v]
  (and (vector? v)
       (string? (first v))
       (not (string/blank? (first v)))))

(defn map-vals [f m]
  (->> (map (fn [[k v]] [k (f v)]) m)
       (into {})))

(defn map-keys [f m]
  (->> (map (fn [[k v]] [(f k) v]) m)
       (into {})))

(defn wrap [val]
  (if (sequential? val)
    val
    [val]))

(defn flat [coll]
  (mapcat #(if (sequential? %) % [%]) coll))

(defn debug [val]
  (println val)
  val)

(defn namespace* [val]
  (when (qualified-ident? val)
    (namespace val)))

(defn name* [val]
  (when (ident? val)
    (name val)))
