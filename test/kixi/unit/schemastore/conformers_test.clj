(ns kixi.unit.schemastore.conformers-test
  (:require [kixi.datastore.schemastore :as ss]
            [kixi.datastore.schemastore.conformers :as conformers]
            [kixi.integration.base :refer :all]
            [clojure.spec :as s]
            [clojure.test :refer :all]))

(defn valid?
  [p r]
  (if (= r :clojure.spec/invalid)
    false
    (if (not (p r))
      (throw (Exception. (format "Conformer returned a result but failed predicate (%s)" r)))
      true)))

(deftest str-double->int
  (is (= 1 (conformers/str-double->int "1.")))
  (is (= 1 (conformers/str-double->int "1.0")))
  (is (= 1 (conformers/str-double->int "1.0000000000")))
  (is (nil? (conformers/str-double->int "1")))
  (is (nil? (conformers/str-double->int "a1.0")))
  (is (nil? (conformers/str-double->int "1.01")))
  (is (nil? (conformers/str-double->int "1.00000000000001"))))

(deftest double->int
  (is (= 1 (conformers/double->int 1.)))
  (is (= 1 (conformers/double->int 1.0)))
  (is (= 1 (conformers/double->int 1.00000000000)))
  (is (nil? (conformers/double->int 1.000000000001))))

(deftest integer-conformer
  (is (valid? int? (s/conform conformers/integer? 1)))
  (is (valid? int? (s/conform conformers/integer? "1")))
  (is (valid? int? (s/conform conformers/integer? 1.0)))   ;; we conform ONLY if .0
  (is (valid? int? (s/conform conformers/integer? "1.0"))) ;; we remove the .0 then conform
  ;;
  (is (not (valid? int? (s/conform conformers/integer? "x"))))
  (is (not (valid? int? (s/conform conformers/integer? true))))
  (is (not (valid? int? (s/conform conformers/integer? 1.0002)))))

(deftest double-conformer
  (is (valid? double? (s/conform conformers/double? 1.23)))
  (is (valid? double? (s/conform conformers/double? "1.23")))
  (is (valid? double? (s/conform conformers/double? 1)))   ;; we conform 1 -> 1.0
  (is (valid? double? (s/conform conformers/double? "1"))) ;; works fine with (Double/valueOf "1")
  ;;
  (is (not (valid? double? (s/conform conformers/double? true))))
  (is (not (valid? double? (s/conform conformers/double? "x")))))

(deftest integer-range-conformer
  (is (valid? int? (s/conform (conformers/integer-range? 3 10) 5)))
  (is (valid? int? (s/conform (conformers/integer-range? 3 10) "5")))
  ;;
  (is (not (valid? int? (s/conform (conformers/integer-range? 3 10) 20))))
  (is (not (valid? int? (s/conform (conformers/integer-range? 3 10) "20"))))
  (is (not (valid? int? (s/conform (conformers/integer-range? 3 10) "x"))))
  (is (not (valid? int? (s/conform (conformers/integer-range? 3 10) 19.95))))
  (is (not (valid? int? (s/conform (conformers/integer-range? 3 10) 4.95))))
  ;;
  (is (thrown-with-msg? IllegalArgumentException #"Both min and max must be integers"
                        (s/conform (conformers/integer-range? "3" 10) 5)))
  (is (thrown-with-msg? IllegalArgumentException #"Both min and max must be integers"
                        (s/conform (conformers/integer-range? 3 "10") 5)))
  (is (thrown-with-msg? IllegalArgumentException #"Both min and max must be integers"
                        (s/conform (conformers/integer-range? 3.0 10.0) 5))))

(deftest double-range-conformer
  (is (valid? double? (s/conform (conformers/double-range? 3.0 10.0) 5.0)))
  (is (valid? double? (s/conform (conformers/double-range? 3.0 10.0) "5.0")))
  (is (valid? double? (s/conform (conformers/double-range? 3.0 10.0) 5)))   ;; conforms to double
  (is (valid? double? (s/conform (conformers/double-range? 3.0 10.0) "5"))) ;; conforms to double
  ;;
  (is (not (valid? double? (s/conform (conformers/double-range? 3.0 10.0) 20.0))))
  (is (not (valid? double? (s/conform (conformers/double-range? 3.0 10.0) "20.0"))))
  (is (not (valid? double? (s/conform (conformers/double-range? 3.0 10.0) "x"))))
  (is (not (valid? double? (s/conform (conformers/double-range? 3.0 10.0) 19))))
  ;;
  (is (thrown-with-msg? IllegalArgumentException #"Both min and max must be doubles"
                        (s/conform (conformers/double-range? "3.0" 10.0) 5.0)))
  (is (thrown-with-msg? IllegalArgumentException #"Both min and max must be doubles"
                        (s/conform (conformers/double-range? 3.0 "10.0") 5.0)))
  (is (thrown-with-msg? IllegalArgumentException #"Both min and max must be doubles"
                        (s/conform (conformers/double-range? 3 1) 5.0))))

(deftest set-conformer
  (is (valid? int?     (s/conform (conformers/set? 1 2 3) 2)))
  (is (valid? keyword? (s/conform (conformers/set? :a :b :c) :b)))
  (is (valid? string?  (s/conform (conformers/set? "a" "b" "c") "b")))
  ;;
  (is (not (valid? int?     (s/conform (conformers/set? 1 2 3) 4))))
  (is (not (valid? keyword? (s/conform (conformers/set? :a :b :c) :x))))
  (is (not (valid? string?  (s/conform (conformers/set? "a" "b" "c") "x")))))

(deftest regex-conformer
  (is (valid? string? (s/conform (conformers/regex? "foobar") "foobar")))
  (is (valid? string? (s/conform (conformers/regex? #"foobar") "foobar")))
  (is (valid? string? (s/conform (conformers/regex? #".*") "aquickbrownfoxjumpsoverthelazydog123456789")))
  ;;
  (is (not (valid? string? (s/conform (conformers/regex? "foo") "bar"))))
  (is (not (valid? string? (s/conform (conformers/regex? #"foo") "bar"))))
  ;;
  (is (thrown-with-msg? IllegalArgumentException #"123 is not a valid regex"
                        (s/conform (conformers/regex? 123) "bar")))
  (is (thrown-with-msg? IllegalArgumentException #"\\k is not a valid regex"
                        (s/conform (conformers/regex? "\\k") "bar"))))

(deftest string-conformer
  (is (valid? string? (s/conform conformers/string? "hello")))
  (is (not (valid? string? (s/conform conformers/string? 123))))
  (is (not (valid? string? (s/conform conformers/string? \g)))))
