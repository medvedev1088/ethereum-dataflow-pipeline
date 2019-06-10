package io.blockchainetl.analyticsdemo.service;

import java.math.BigDecimal;
import java.math.BigInteger;

public interface PercentileService {

    BigInteger getEtherPercentile(BigDecimal percentile, Long periodDays);
}
