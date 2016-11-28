#!/bin/bash
for f in $(ls *.scala) ; do
    filename=$(basename "$file")
    #echo filename="${f%.*}"
    echo extension="${f##*.}"
    #cat ${f} | sed 's/0/1/g; s/16777215/0/g' > $
	filename="${f%.*}"
    #cat ${f} | sed 's/D:\/Home\/Workspaces\/Spark\/DataFrame/./g' > ${filename}.tmp
    #cat ${f} | sed 's/file:\/\//file:\/\/\//g' > ${filename}.tmp
    cat ${f} | sed 's/file:\/\/\//file:\/\/\//g' > ${filename}.tmp
	mv ${filename}.tmp ${filename}.scala
done
