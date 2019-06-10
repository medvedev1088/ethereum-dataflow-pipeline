#!/usr/bin/env bash

mvn -Pdirect-runner compile exec:java \
-Dexec.mainClass=io.blockchainetl.analyticsdemo.LargeTransactionsPipeline \
-Dexec.args="\
--inputSubscription=projects/<your_project>/subscriptions/crypto_ethereum.dataflow.transactions.large_transactions \
--outputTopic=projects/<your_project>/topics/crypto_ethereum.large_transactions \
--tempLocation=gs://<your_project>-dataflow-temp/dataflow"

