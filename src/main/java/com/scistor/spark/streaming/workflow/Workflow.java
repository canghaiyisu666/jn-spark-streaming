package com.scistor.spark.streaming.workflow;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Wei Xing
 */
public class Workflow {

    private Map<String, Operator> operators;

    public Workflow() {
        this.operators = new HashMap<>(20);
    }

    public Map<String, Operator> getOperators() {
        return operators;
    }

    public void setOperators(Map<String, Operator> operators) {
        this.operators = operators;
    }

    public void addOperator(Operator operator) {
        operators.put(operator.getId(), operator);
    }
}
