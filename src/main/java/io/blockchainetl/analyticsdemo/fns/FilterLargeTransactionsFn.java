package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.domain.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;

public class FilterLargeTransactionsFn extends ErrorHandlingDoFn<Transaction, Transaction> {

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
            c.output(transaction);
        }
    }
}
