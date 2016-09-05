#!/bin/bash

dd if=/dev/zero of=10B-file.txt count=1 bs=10;

dd if=/dev/zero of=10MB-file.txt count=1048576 bs=10;

dd if=/dev/zero of=300MB-file.txt count=31457280 bs=10;
