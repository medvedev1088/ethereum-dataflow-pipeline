package io.blockchainetl.analyticsdemo;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface AnalyticsDemoPipelineOptions extends PipelineOptions, StreamingOptions, SdkHarnessOptions {

    @Description("Input PubSub subscription")
    @Validation.Required
    String getInputSubscription();

    void setInputSubscription(String value);
    
    @Description("Output PubSub topic")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String value);
}
