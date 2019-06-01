package io.blockchainetl.analyticsdemo.domain;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.math.BigInteger;

@DefaultCoder(AvroCoder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionWindow {

    @Nullable
    @JsonProperty("timestamp")
    private String timestamp;
    
    @Nullable
    @JsonProperty("from_address_label")
    private String fromAddressLabel;

    @Nullable
    @JsonProperty("to_address_label")
    private String toAddressLabel;

    @Nullable
    private BigInteger volume;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getFromAddressLabel() {
        return fromAddressLabel;
    }

    public void setFromAddressLabel(String fromAddressLabel) {
        this.fromAddressLabel = fromAddressLabel;
    }

    public String getToAddressLabel() {
        return toAddressLabel;
    }

    public void setToAddressLabel(String toAddressLabel) {
        this.toAddressLabel = toAddressLabel;
    }

    public BigInteger getVolume() {
        return volume;
    }

    public void setVolume(BigInteger volume) {
        this.volume = volume;
    }

    public TransactionWindow() {}
}
