#!/usr/bin/env bash

mvn -Pdirect-runner compile exec:java \
-Dexec.mainClass=io.blockchainetl.analyticsdemo.AnalyticsDemoPipeline \
-Dexec.args="\
--inputSubscription=projects/<your_project>/subscriptions/crypto_ethereum.dataflow.transactions.analytics_demo \
--outputTopic=projects/<your_project>/topics/crypto_ethereum.analytics_demo \
--tempLocation=gs://<your_project>-dataflow-temp/dataflow"

