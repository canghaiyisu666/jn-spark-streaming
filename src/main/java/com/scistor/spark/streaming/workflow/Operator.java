package com.scistor.spark.streaming.workflow;

import java.util.Properties;
import java.util.Set;

/**
 * @author Wei Xing
 */
public interface Operator {

    public String getId();

    public String getName();

    public Set<String> getInputs();

    public Properties getProps();
}
