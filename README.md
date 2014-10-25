
#Overview
This is the hadoop hello world WordCount using maven as building tool.

#Build
mvn package 

#Use local runner

N.B.
You are required to have hadoop installed on your machine

cd target
hadoop jar maven-hadoop-1.0-SNAPSHOT.jar it.rome.saiello.WordCount -jt local -fs file:/// ../data/in/Ulysses.txt out