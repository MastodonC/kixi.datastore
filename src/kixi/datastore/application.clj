(ns kixi.datastore.application)


;; This is NOT the namespace you are looking for.

;; it's critical that no requires, or other code is added to this namespace.
;; see dev/user.clj


;; holder for a single instance of the application.
;; We create this here to allow reference from a cider(emacs) repl or a repl
;; started from a main method.
;;
;; See https://github.com/stuartsierra/component
(def system (atom nil))
(def profile (atom nil))
