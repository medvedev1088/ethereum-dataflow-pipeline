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

    @Override
    public BigInteger getEtherPercentile(BigDecimal percentile, Long periodDays) {
        BigDecimal percentileAsFraction = percentile.divide(BigDecimal.valueOf(100), 10, BigDecimal.ROUND_HALF_DOWN);
        String query = String.format("select PERCENTILE_CONT(value, %s) OVER() AS percentile_value\n"
            + "from `bigquery-public-data.crypto_ethereum.transactions` as t\n"
            + "where DATE(block_timestamp) > DATE_ADD(CURRENT_DATE() , INTERVAL -%s DAY)\n"
            + "limit 1", percentileAsFraction, periodDays);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        TableResult tableResult;
        try {
            LOG.info("Calling BigQuery: " + query);
            tableResult = bigquery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Got results with size: " + tableResult.getTotalRows());


        for (FieldValueList row : tableResult.iterateAll()) {
            FieldValue percentileValue = row.get("percentile_value");

            if (percentileValue == null || percentileValue.getNumericValue() == null) {
                throw new IllegalArgumentException("Value in null in BigQuery result");
            }

            BigDecimal numericValue = percentileValue.getNumericValue();

            return numericValue.toBigInteger();
        }

        throw new IllegalArgumentException("No value in BigQuery result");
    }
}
