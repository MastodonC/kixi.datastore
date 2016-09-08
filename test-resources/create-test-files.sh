#!/bin/bash

rm -rg sample-files-full
mkdir sample-files-full

mkdir sample-files-heads

IFS=$'\n'       # make newlines the only separator
set -f          # disable globbing
for line in $(cat ./sample-files.csv | tail -n +2); do
  name=`echo $line | cut -f 2 -d ,`
  url=`echo $line | cut -f 3 -d ,`
  url=${url#"https://"} #remove prefix
  url=${url//:/%3A} #urlencode :'s
  url="https://$url" #return prefix
  url="$(echo $url | sed 's/[\r\n]//')" #remove any line endings
  echo "Downloading: $name - $url"
  curl -o "./sample-files-full/$name" $url
#  head -n 10 "./sample-files-full/$name" > "./sample-files-heads/$name"
done


dd if=/dev/zero of=10B-file.txt count=1 bs=10;

dd if=/dev/zero of=10MB-file.txt count=1048576 bs=10;

dd if=/dev/zero of=300MB-file.txt count=31457280 bs=10;
