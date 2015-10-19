package com.scistor.spark.streaming.workflow.json;

import com.scistor.spark.streaming.workflow.Workflow;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;

/**
 * @author Wei Xing
 */
public class WorkflowFactoryTest {
    @Test
    public void testGetWorkflow() throws IOException {
        String definition = IOUtils.toString(this.getClass().getResourceAsStream("/workflow.json"));
        Workflow workflow = new WorkflowFactory().getWorkflow(definition);
        System.out.println(workflow.getOperators());

        assertEquals(2, workflow.getOperators().size());
        assertTrue(workflow.getOperators().get("1").getName().equals("RocketMQReader"));
        assertTrue(workflow.getOperators().get("2").getName().equals("HiveWriter"));
    }
}
