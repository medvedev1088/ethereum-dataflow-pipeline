package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.domain.LargeTransactionMessage;
import io.blockchainetl.analyticsdemo.domain.Transaction;
import io.blockchainetl.analyticsdemo.service.PercentileServiceHolder;

import java.math.BigDecimal;

public class BuildLargeTransactionMessagesFn extends ErrorHandlingDoFn<Transaction, LargeTransactionMessage> {

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        Transaction transaction = c.element();

        LargeTransactionMessage message = new LargeTransactionMessage();

        message.setTransaction(transaction);
        message.setPercentile(new BigDecimal("99.9"));
        message.setPercentileValue(PercentileServiceHolder.INSTANCE.getEtherPercentile());
        message.setPercentilePeriodDays(30L);
        message.setPercentilePeriodBlocks(185525L);

        c.output(message);
    }
}
