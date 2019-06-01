package io.blockchainetl.analyticsdemo.fns;

import io.blockchainetl.analyticsdemo.domain.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class AddAddressLabelsFn extends ErrorHandlingDoFn<Transaction, Transaction> {

    private static final String UNKNOWN = "unknown";
    
    private final PCollectionView<Map<String, String>> addressLabelsSideInput;

    public AddAddressLabelsFn(PCollectionView<Map<String, String>> addressLabelsSideInput) {
        this.addressLabelsSideInput = addressLabelsSideInput;
    }

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        Transaction transaction = c.element().clone();
        Map<String, String> addressLabels = c.sideInput(this.addressLabelsSideInput);
        
        String fromAddressLabel = addressLabels.get(transaction.getFromAddress());
        if (fromAddressLabel == null || fromAddressLabel.isEmpty()) {
            fromAddressLabel = UNKNOWN;
        }
        
        String toAddressLabel = addressLabels.get(transaction.getToAddress());
        if (toAddressLabel == null || toAddressLabel.isEmpty()) {
            toAddressLabel = UNKNOWN;
        }

        transaction.setFromAddressLabel(fromAddressLabel);
        transaction.setToAddressLabel(toAddressLabel);

        c.output(transaction);
    }
}
