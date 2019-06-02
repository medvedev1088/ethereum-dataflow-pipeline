package io.blockchainetl.analyticsdemo;

import com.google.api.services.bigquery.model.TableRow;
import io.blockchainetl.analyticsdemo.domain.Transaction;
import io.blockchainetl.analyticsdemo.domain.TransactionWindow;
import io.blockchainetl.analyticsdemo.fns.AddAddressLabelsFn;
import io.blockchainetl.analyticsdemo.fns.AddGroupFn;
import io.blockchainetl.analyticsdemo.fns.AddTimestampsFn;
import io.blockchainetl.analyticsdemo.fns.AddressLabelsToMapFn;
import io.blockchainetl.analyticsdemo.fns.BuildTransactionWindowsFn;
import io.blockchainetl.analyticsdemo.fns.EncodeToJsonFn;
import io.blockchainetl.analyticsdemo.fns.LogElementsFn;
import io.blockchainetl.analyticsdemo.fns.ParseEntitiesFromJsonFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


public class AnalyticsDemoPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(AnalyticsDemoPipeline.class);
    
    private static final String PUBSUB_ID_ATTRIBUTE = "item_id";

    public static void main(String[] args) throws IOException, InterruptedException {
        AnalyticsDemoPipelineOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(AnalyticsDemoPipelineOptions.class);

        runPipeline(options);
    }

    public static void runPipeline(
        AnalyticsDemoPipelineOptions options
    ) {
        Pipeline p = Pipeline.create(options);

        // Read input
        
        PCollection<String> input = p.apply("ReadFromPubSub",
            PubsubIO.readStrings()
                .fromSubscription(options.getInputSubscription())
                .withIdAttribute(PUBSUB_ID_ATTRIBUTE));
        
        // Build pipeline
        
        PCollection<String> output = buildPipeline(addressLabels(p), input);
        
        // Write output
        
        output
            .apply("LogElements", ParDo.of(new LogElementsFn<>("Window: ")))
            .apply("WriteElements", PubsubIO.writeStrings().to(options.getOutputTopic()));

        // Run pipeline

        PipelineResult pipelineResult = p.run();
        LOG.info(pipelineResult.toString());
        pipelineResult.waitUntilFinish();
    }

    public static PCollection<String> buildPipeline(
        PCollectionView<Map<String, String>> addressLabels, 
        PCollection<String> input) {
        // Add timestamps
        PCollection<String> inputWithTimestamps = input
            .apply("AddTimestamps", ParDo.of(new AddTimestampsFn()));

        // Parse transactions

        PCollection<Transaction> transactions = inputWithTimestamps
            .apply("ParseTransactions", ParDo.of(new ParseEntitiesFromJsonFn<>(Transaction.class)))
            .setCoder(AvroCoder.of(Transaction.class));

        // Transactions with labels

        PCollection<Transaction> enrichedTransactions = transactions
            .apply("AddAddressLabels",
                ParDo.of(new AddAddressLabelsFn(addressLabels)).withSideInputs(addressLabels));

        // Window

        PCollection<Transaction> window = enrichedTransactions
            .apply("AddWindows",
                Window.<Transaction>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
                    .withAllowedLateness(Duration.standardHours(2))
                    .accumulatingFiredPanes()
            );

        // Grouping

        PCollection<KV<String, Iterable<Transaction>>> grouped = window
            .apply(ParDo.of(new AddGroupFn()))
            .apply("GroupTransactions", GroupByKey.create());

        PCollection<TransactionWindow> transactionWindows = grouped
            .apply(ParDo.of(new BuildTransactionWindowsFn()));


        // Encode to JSON

        return transactionWindows
            .apply("EncodeToJson", ParDo.of(new EncodeToJsonFn()));
    }

    private static PCollectionView<Map<String, String>> addressLabels(Pipeline p) {
        String addressLabelsQuery = "SELECT address, label "
            + "FROM `crypto-etl-ethereum-dev.dataflow_sql.all_labels`";

        PCollection<TableRow> rows = p.apply("AddressLabels",
            BigQueryIO.readTableRows().fromQuery(addressLabelsQuery)
                .usingStandardSql()
                .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE));

        PCollectionView<Map<String, String>> result = addressLabelRowsToView(rows);

        return result;
    }

    public static PCollectionView<Map<String, String>> addressLabelRowsToView(PCollection<TableRow> rows) {
        return rows
                .apply("TransformToKeyValues", ParDo.of(new AddressLabelsToMapFn()))
                .apply("TransformToView", View.asMap());
    }
}
