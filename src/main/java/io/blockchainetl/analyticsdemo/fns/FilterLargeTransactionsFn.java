package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.Constants;
import io.blockchainetl.analyticsdemo.domain.LargeTransactionMessage;
import io.blockchainetl.analyticsdemo.domain.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;

public class FilterLargeTransactionsFn extends ErrorHandlingDoFn<Transaction, LargeTransactionMessage> {

    private final PCollectionView<BigInteger> etherPercentileSideInput;

    public FilterLargeTransactionsFn(PCollectionView<BigInteger> etherPercentileSideInput) {
        this.etherPercentileSideInput = etherPercentileSideInput;
    }

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        BigInteger etherPercentile = c.sideInput(this.etherPercentileSideInput);

        Transaction transaction = c.element().clone();
        BigInteger value = transaction.getValue();

        if (value.compareTo(etherPercentile) > 0) {
            LargeTransactionMessage message = new LargeTransactionMessage();

            message.setTransaction(transaction);
            message.setPercentile(Constants.ETHER_PERCENTILE);
            message.setPercentileValue(etherPercentile);
            message.setPercentilePeriodDays(Constants.ETHER_PERCENTILE_PERIOD_DAYS);
            message.setPercentilePeriodBlocks(Constants.ETHER_PERCENTILE_PERIOD_BLOCKS);
            c.output(message);
        }
    }
}
