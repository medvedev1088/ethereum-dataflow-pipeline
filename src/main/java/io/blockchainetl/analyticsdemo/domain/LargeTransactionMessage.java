package io.blockchainetl.analyticsdemo.domain;

import com.google.common.base.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.math.BigDecimal;
import java.math.BigInteger;

@DefaultCoder(AvroCoder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LargeTransactionMessage {

    @Nullable
    private Transaction transaction;

    @Nullable
    private BigDecimal percentile;

    @Nullable
    @JsonProperty("percentile_value")
    private BigInteger percentileValue;

    @Nullable
    @JsonProperty("percentile_period_days")
    private Long percentilePeriodDays;

    @Nullable
    @JsonProperty("percentile_period_blocks")
    private Long percentilePeriodBlocks;

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public BigDecimal getPercentile() {
        return percentile;
    }

    public void setPercentile(BigDecimal percentile) {
        this.percentile = percentile;
    }

    public BigInteger getPercentileValue() {
        return percentileValue;
    }

    public void setPercentileValue(BigInteger percentileValue) {
        this.percentileValue = percentileValue;
    }

    public Long getPercentilePeriodDays() {
        return percentilePeriodDays;
    }

    public void setPercentilePeriodDays(Long percentilePeriodDays) {
        this.percentilePeriodDays = percentilePeriodDays;
    }

    public Long getPercentilePeriodBlocks() {
        return percentilePeriodBlocks;
    }

    public void setPercentilePeriodBlocks(Long percentilePeriodBlocks) {
        this.percentilePeriodBlocks = percentilePeriodBlocks;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("transaction", transaction)
            .add("percentile", percentile)
            .add("percentileValue", percentileValue)
            .add("percentilePeriodDays", percentilePeriodDays)
            .add("percentilePeriodBlocks", percentilePeriodBlocks)
            .toString();
    }
}
