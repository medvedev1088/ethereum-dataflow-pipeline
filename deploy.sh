#!/usr/bin/env bash

mvn -e -Pdataflow-runner compile exec:java \
-Dexec.mainClass=io.blockchainetl.analyticsdemo.AnalyticsDemoPipeline \
-Dexec.args="\
--inputSubscription=projects/<your_project>/subscriptions/crypto_ethereum.dataflow.transactions.analytics-demo \
--outputTopic=projects/<your_project>/topics/crypto_ethereum.analytics-demo \
--gcpTempLocation=gs://<your_project>-dataflow-temp/dataflow \
--tempLocation=gs://<your_project>-dataflow-temp/dataflow \
--project=<your_project> \
--runner=DataflowRunner \
--jobName=ethereum-analytics-demo-0 \
--workerMachineType=n1-standard-1 \
--maxNumWorkers=1 \
--diskSizeGb=30 \
--region=us-central1 \
--zone=us-central1-a \
"
