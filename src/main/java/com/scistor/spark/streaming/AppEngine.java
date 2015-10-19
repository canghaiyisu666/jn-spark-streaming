package com.scistor.spark.streaming;

import com.google.common.collect.Maps;
import com.scistor.spark.streaming.operator.common.*;
import com.scistor.spark.streaming.workflow.Operator;
import com.scistor.spark.streaming.workflow.Workflow;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Map;
import java.util.Set;

/**
 * @author Wei Xing
 */
public class AppEngine {

    private JavaStreamingContext jsc;
    private Map<String, Operator> operators;
    private Map<String, SchemaDStream> outputs;

    public AppEngine(JavaStreamingContext jsc, Workflow workflow) {
        this.jsc = jsc;
        this.operators = workflow.getOperators();
        this.outputs = Maps.newHashMapWithExpectedSize(operators.size());
    }

    public void setUp() throws Exception {
        for (String id : operators.keySet()) {
            process(id);
        }
    }

    private void process(String id) throws Exception {
        if (!isProcessed(id)) {
            validParentProcessed(id);

            Operator operator = operators.get(id);
            Object processor = Class.forName("com.scistor.spark.streaming.operator." + operator.getName()).newInstance();
            if (processor instanceof IReader) {
                outputs.put(id, ((IReader) processor).read(jsc, operator.getProps()));
            } else if (processor instanceof ITransform) {
                SchemaDStream output = outputs.get(operator.getInputs().toArray()[0]);
                outputs.put(id, ((ITransform) processor).transform(output, operator.getProps()));
            } else if (processor instanceof IWriter) {
                SchemaDStream output = outputs.get(operator.getInputs().toArray()[0]);
                ((IWriter) processor).write(output, operator.getProps());
            }
        }
    }

    private void validParentProcessed(String id) throws Exception {
        Set<String> inputs = operators.get(id).getInputs();
        if (inputs != null && inputs.size() != 0) {
            for (String input : inputs) {
                if (!isProcessed(input)) {
                    process(input);
                }
            }
        }
    }

    private boolean isProcessed(String id) {
        return outputs.containsKey(id);
    }
}
