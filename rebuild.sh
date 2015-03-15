#!/bin/sh
cd ~/git/presto/presto-main/
mvn clean install -DskipTests
cd ~/git/presto/presto-server/
mvn clean install -DskipTests
cp -r ~/git/presto/etc ~/git/presto/presto-server/target/presto-server-0.88-SNAPSHOT
~/git/presto/presto-server/target/presto-server-0.88-SNAPSHOT/bin/launcher restart
