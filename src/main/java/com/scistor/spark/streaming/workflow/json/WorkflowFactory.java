package com.scistor.spark.streaming.workflow.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.scistor.spark.streaming.workflow.Workflow;

/**
 * @author Wei Xing
 */
public class WorkflowFactory {

    public Workflow getWorkflow(String definition) {
        try {
            OperatorImpl[] operators = new ObjectMapper().readValue(definition, OperatorImpl[].class);
            Workflow workflow = new Workflow();
            for (OperatorImpl operator : operators) {
                workflow.addOperator(operator);
            }
            return workflow;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
