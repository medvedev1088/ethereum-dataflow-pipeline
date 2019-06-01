package io.blockchainetl.analyticsdemo.fns;


import io.blockchainetl.analyticsdemo.utils.JsonUtils;

public class EncodeToJsonFn extends ErrorHandlingDoFn<Object, String> {

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        Object elem = c.element();
        c.output(JsonUtils.encodeJson(elem));
    }
}
