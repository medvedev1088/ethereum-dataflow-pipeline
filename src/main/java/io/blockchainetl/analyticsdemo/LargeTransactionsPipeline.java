package io.blockchainetl.analyticsdemo;

import io.blockchainetl.analyticsdemo.domain.LargeTransactionMessage;
import io.blockchainetl.analyticsdemo.domain.Transaction;
import io.blockchainetl.analyticsdemo.fns.AddTimestampsFn;
import io.blockchainetl.analyticsdemo.fns.BuildLargeTransactionMessagesFn;
import io.blockchainetl.analyticsdemo.fns.EncodeToJsonFn;
import io.blockchainetl.analyticsdemo.fns.FilterLargeTransactionsFn;
import io.blockchainetl.analyticsdemo.fns.LogElementsFn;
import io.blockchainetl.analyticsdemo.fns.ParseEntitiesFromJsonFn;
import io.blockchainetl.analyticsdemo.service.PercentileServiceHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;


public class LargeTransactionsPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(LargeTransactionsPipeline.class);

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

        PCollection<String> output = buildPipeline(etherPercentile(p), input);

        // Write output

        output
            .apply("LogElements", ParDo.of(new LogElementsFn<>("LargeTransactionMessage: ")))
            .apply("WriteElements", PubsubIO.writeStrings().to(options.getOutputTopic()));

        // Run pipeline

        PipelineResult pipelineResult = p.run();
        LOG.info(pipelineResult.toString());
        pipelineResult.waitUntilFinish();
    }

    public static PCollection<String> buildPipeline(
        PCollectionView<BigInteger> etherPercentile,
        PCollection<String> input
    ) {
        // Add timestamps
        PCollection<String> inputWithTimestamps = input
            .apply("AddTimestamps", ParDo.of(new AddTimestampsFn()));

        // Parse transactions

        PCollection<Transaction> transactions = inputWithTimestamps
            .apply("ParseTransactions", ParDo.of(new ParseEntitiesFromJsonFn<>(Transaction.class)))
            .setCoder(AvroCoder.of(Transaction.class));

        // Large transactions

        PCollection<Transaction> largeTransactions = transactions
            .apply("FilterLargeTransactions",
                ParDo.of(new FilterLargeTransactionsFn(etherPercentile)).withSideInputs(etherPercentile));

        // Build message

        PCollection<LargeTransactionMessage> transactionWindows = largeTransactions
            .apply(ParDo.of(new BuildLargeTransactionMessagesFn()));


        // Encode to JSON

        return transactionWindows
            .apply("EncodeToJson", ParDo.of(new EncodeToJsonFn()));
    }

    private static PCollectionView<BigInteger> etherPercentile(Pipeline p) {
        PCollectionView<BigInteger> etherPercentile =
            p.apply(GenerateSequence.from(0).withRate(1, Duration.standardDays(1L)))
                .apply(Window.<Long>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes()
                )
                .apply(
                    ParDo.of(
                        new DoFn<Long, BigInteger>() {

                            @ProcessElement
                            public void process(@Element Long input, DoFn.OutputReceiver<BigInteger> o) {
                                o.output(PercentileServiceHolder.INSTANCE.getEtherPercentile(
                                    Constants.ETHER_PERCENTILE, Constants.ETHER_PERCENTILE_PERIOD_DAYS));
                            }
                        }))
                .apply(View.asSingleton());
        return etherPercentile;
    }
}
