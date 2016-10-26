#!/bin/bash

DIR=`dirname $0`

echo $DIR

cd $DIR

#rm -rg sample-files-full
#mkdir sample-files-full

#mkdir sample-files-heads

#IFS=$'\n'       # make newlines the only separator
#set -f          # disable globbing
#for line in $(cat ./sample-files.csv | tail -n +2); do
#  name=`echo $line | cut -f 2 -d ,`
#  url=`echo $line | cut -f 3 -d ,`
#  url=${url#"https://"} #remove prefix
#  url=${url//:/%3A} #urlencode :'s
#  url="https://$url" #return prefix
# url="$(echo $url | sed 's/[\r\n]//')" #remove any line endings
# echo "Downloading: $name - $url"
# curl -o "./sample-files-full/$name" $url
#  head -n 10 "./sample-files-full/$name" > "./sample-files-heads/$name"
#one

cat header-metadata.csv > ./metadata-one-valid.csv
cat valid-line-metadata.csv >> ./metadata-one-valid.csv

cat valid-line-metadata.csv >> ./metadata-one-valid-no-header.csv

cat header-metadata.csv > ./metadata-one-invalid.csv
cat invalid-line-metadata.csv >> ./metadata-one-invalid.csv

cat header-metadata.csv > ./metadata-12MB-valid.csv
(perl -0777pe '$_=$_ x 3000000' valid-line-metadata.csv) >> ./metadata-12MB-valid.csv

cat header-metadata.csv > ./metadata-344MB-valid.csv
(perl -0777pe '$_=$_ x 90000000' valid-line-metadata.csv) >> ./metadata-344MB-valid.csv

cat header-metadata.csv > ./metadata-344MB-invalid.csv
(perl -0777pe '$_=$_ x 90000000' invalid-line-metadata.csv) >> ./metadata-344MB-invalid.csv

cd ..
