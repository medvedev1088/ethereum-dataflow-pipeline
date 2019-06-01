package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.domain.Transaction;
import org.apache.beam.sdk.values.KV;

public class AddGroupFn extends ErrorHandlingDoFn<Transaction, KV<String, Transaction>> {

    public static final String SEPARATOR = "__";

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        Transaction transaction = c.element();
        String group = transaction.getFromAddressLabel() + SEPARATOR + transaction.getToAddressLabel();
        c.output(KV.of(group, transaction));
    }
}
