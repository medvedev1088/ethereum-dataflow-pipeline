package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.domain.Transaction;
import io.blockchainetl.analyticsdemo.domain.TransactionWindow;
import io.blockchainetl.analyticsdemo.utils.TimeUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.stream.StreamSupport;

public class BuildTransactionWindowsFn extends DoFn<KV<String, Iterable<Transaction>>, TransactionWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(BuildTransactionWindowsFn.class);

    @ProcessElement
    public void processElement(ProcessContext c, BoundedWindow window) throws Exception {
        try {
            doProcessElement(c, window);
        } catch (Exception e) {
            // https://cloud.google.com/blog/products/gcp/handling-invalid-inputs-in-dataflow
            LOG.error("Failed to process input {}.", c.element(), e);
            // OutOfMemoryError should be retried
            if (e.getCause() instanceof OutOfMemoryError ||
                (e.getCause() != null && e.getCause().getCause() instanceof OutOfMemoryError)) {
                throw e;
            }
        }
    }

    protected void doProcessElement(ProcessContext c, BoundedWindow window) {
        Iterable<Transaction> transactions = c.element().getValue();
        BigInteger valueSum = StreamSupport.stream(transactions.spliterator(), false).map(
            Transaction::getValue).reduce(BigInteger.ZERO, BigInteger::add);

        TransactionWindow transactionWindow = new TransactionWindow();
        transactionWindow.setTimestamp(TimeUtils.formatTimestamp(window.maxTimestamp().getMillis() / 1000));
        transactionWindow.setVolume(valueSum);

        if (c.element().getKey() == null) {
            throw new IllegalArgumentException("Group is null");
        }
        
        String[] labels = c.element().getKey().split(AddGroupFn.SEPARATOR);
        
        if (labels.length < 2) {
            throw new IllegalArgumentException("Group is invalid " + c.element().getKey());
        }
        
        String fromAddressLabel = labels[0]; 
        String toAddressLabel = labels[1];
        
        transactionWindow.setFromAddressLabel(fromAddressLabel);
        transactionWindow.setToAddressLabel(toAddressLabel);

        c.output(transactionWindow);
    }
}
