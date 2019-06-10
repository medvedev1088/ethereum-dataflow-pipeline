package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.domain.Transaction;
import io.blockchainetl.analyticsdemo.service.PercentileServiceHolder;

import java.math.BigInteger;

public class FilterLargeTransactionsFn extends ErrorHandlingDoFn<Transaction, Transaction> {

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        Transaction transaction = c.element().clone();

        BigInteger value = transaction.getValue();
        BigInteger etherPercentile = PercentileServiceHolder.INSTANCE.getEtherPercentile();

        if (value.compareTo(etherPercentile) > 0) {
            c.output(transaction);
        }
    }
}
