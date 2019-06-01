package io.blockchainetl.analyticsdemo.fns;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class AddressLabelsToMapFn extends DoFn<TableRow, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        TableRow row = c.element();
        String address = (String) row.get("address");
        String label = (String) row.get("label");
        if (address != null && !address.isEmpty() && label != null && !label.isEmpty()) {
            c.output(KV.of(address, label));
        }
    }

}
