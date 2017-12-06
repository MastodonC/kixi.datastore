# kixi.datastore

## Using the REPL for administration

Beforehand:

``` clojure
(require '[kixi.datastore.kaylee :as k])
```

**To retrieve all metadata about a file:**

``` clojure
(k/get-metadata "<file-id>")
```

**To get all the metadata visible to a particular group:**

``` clojure
(k/get-metadata-by-group ["<group-id1>" ["<group-id2>" ...] <starting-index> <num-of-results> )
```

Use the starting index and number of results parameters to appropriately page the results.

**To remove sharing information from a file (aka *hide* it):**

```clojure
(k/remove-all-sharing "<your-user-uid>" "<file-id>")
```

## License

Copyright © 2016 Mastodon C

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
