package io.blockchainetl;

import com.google.api.services.bigquery.model.TableRow;
import io.blockchainetl.analyticsdemo.AnalyticsDemoPipeline;
import io.blockchainetl.analyticsdemo.TestUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


@RunWith(JUnit4.class)
public class AnalyticsDemoPipelineTest {

    @Rule
    public TestPipeline p = TestPipeline.create();
    
    @Test
    @Category(ValidatesRunner.class)
    public void testBasic() throws Exception {
        testTemplate(
            "testdata/ethereumBlock1000000Transactions.json",
            "testdata/ethereumBlock1000000TransactionsExpected.json"
        );
    }
    
    private void testTemplate(String inputFile, String outputFile) throws IOException {
        PCollection<TableRow> addressLabels = p.apply("AddressLabels", Create.of(mockAddressLabels()));
        
        List<String> blockchainData = TestUtils.readLines(inputFile);
        PCollection<String> input = p.apply("Input", Create.of(blockchainData));

        PCollection<String> output = AnalyticsDemoPipeline.buildPipeline(
            AnalyticsDemoPipeline.addressLabelRowsToView(addressLabels),
            input
        );

        TestUtils.logPCollection(output);

        PAssert.that(output).containsInAnyOrder(TestUtils.readLines(outputFile));

        p.run().waitUntilFinish();  
    }

    private static List<TableRow> mockAddressLabels() {
        TableRow tr1 = new TableRow();

        tr1.set("address", "0x39fa8c5f2793459d6622857e7d9fbb4bd91766d3");
        tr1.set("label", "binance_user_wallet");

        return Arrays.asList(tr1);
    }
}
