(ns kixi.datastore.schemaextracter-test
  (:require [clojure.test :refer :all]
            [kixi.datastore.schemaextracter :refer :all]))

(deftest basic-selections
  (is (= string
         (schema-for :header "foo")))
  (is (= string
         (schema-for :header "123")))
  (is (= numeric
         (schema-for :value "123"))))

(deftest gss-code-selection
  (is (= gss-code
         (schema-for :header "gss code")))
  (is (= gss-code
         (schema-for :header "gss code")))
  (is (= gss-code
         (schema-for :header "gss.code")))
  (is (= gss-code
         (schema-for :header "GsS-CoDE")))
  (is (= gss-code
         (schema-for :value "E01000000001"))))

(def oa-classification-london-2001-header
  "Output Area,OAC-SuperGroup,OAC-SuperGroup-Name*,OAC-Group,OAC-Group-Name*,OAC-Subgroup,OAC-Subgroup-Name*,Ward-code,Ward-name,Dist-code,Dist-name,Region")

(def oa-classification-london-2001-first-line
  "00AAFA0001,2,City Living,2a,Transient Communities,2a2,Transient Communities (2),00AAFA,Aldersgate,00AA,City of London,London")

(deftest oa-classification-london-2001-schema-extraction
  (is (header-details oa-classification-london-2001-header))
  (is (value-schema oa-classification-london-2001-first-line)))
