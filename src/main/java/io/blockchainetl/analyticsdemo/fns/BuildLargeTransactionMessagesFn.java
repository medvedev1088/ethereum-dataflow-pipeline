package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.Constants;
import io.blockchainetl.analyticsdemo.domain.LargeTransactionMessage;
import io.blockchainetl.analyticsdemo.domain.Transaction;
import io.blockchainetl.analyticsdemo.service.PercentileServiceHolder;

import java.math.BigDecimal;
import java.math.BigInteger;

public class BuildLargeTransactionMessagesFn extends ErrorHandlingDoFn<Transaction, LargeTransactionMessage> {

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        Transaction transaction = c.element();

        LargeTransactionMessage message = new LargeTransactionMessage();

        BigDecimal etherPercentile = Constants.ETHER_PERCENTILE;
        Long etherPercentileDays = Constants.ETHER_PERCENTILE_PERIOD_DAYS;
        BigInteger percentile = PercentileServiceHolder.INSTANCE.getEtherPercentile(etherPercentile, etherPercentileDays);
        
        message.setTransaction(transaction);
        message.setPercentile(etherPercentile);
        message.setPercentileValue(percentile);
        message.setPercentilePeriodDays(etherPercentileDays);
        message.setPercentilePeriodBlocks(Constants.ETHER_PERCENTILE_PERIOD_BLOCKS);

        c.output(message);
    }
}
