package io.blockchainetl.analyticsdemo.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;

public class PercentileServiceImpl implements PercentileService {

    private static final Logger LOG = LoggerFactory.getLogger(PercentileServiceImpl.class);

    private BigInteger cachedPercentile;
    private final Long lock = 0L;

    @Override
    public BigInteger getEtherPercentile() {
        if (cachedPercentile == null) {
            synchronized (lock) {
                if (cachedPercentile == null) {
                    cachedPercentile = doGetEtherPercentile();
                }
            }

        }
        return cachedPercentile;
    }

    private BigInteger doGetEtherPercentile() {
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        String query = "select PERCENTILE_CONT(value, 0.999) OVER() AS percentile99\n"
            + "from `bigquery-public-data.crypto_ethereum.transactions` as t\n"
            + "where DATE(block_timestamp) > DATE_ADD(CURRENT_DATE() , INTERVAL -30 DAY)\n"
            + "limit 1";

        LOG.info("Calling BigQuery: " + query);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        TableResult tableResult;
        try {
            tableResult = bigquery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Got results: " + tableResult.getTotalRows());


        for (FieldValueList row : tableResult.iterateAll()) {
            FieldValue percentile99 = row.get("percentile99");

            if (percentile99 == null || percentile99.getNumericValue() == null) {
                throw new IllegalArgumentException("Value in null in BigQuery result");
            }

            BigDecimal numericValue = percentile99.getNumericValue();

            return numericValue.toBigInteger();
        }

        throw new IllegalArgumentException("No value in BigQuery result");
    }
}
