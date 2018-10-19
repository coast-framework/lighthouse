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

(defn sqlize [k]
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

(defn unqualify-keys [m]
  (->> (map (fn [[k v]] [(-> k name keyword) v]) m)
       (into {})))
