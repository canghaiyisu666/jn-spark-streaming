package com.scistor.spark.streaming.workflow.json;

import com.scistor.spark.streaming.workflow.Operator;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

/**
 * @author Wei Xing
 */
public class OperatorImpl implements Operator {
    private String id;
    private String name;
    private Set<String> inputs = Collections.emptySet();
    private Properties props;

    public OperatorImpl() {
    }

    public OperatorImpl(String id, String name, Set<String> inputs, Properties props) {
        this.id = id;
        this.name = name;
        this.inputs = inputs;
        this.props = props;
    }

    @Override
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public Set<String> getInputs() {
        return inputs;
    }

    public void setParentIds(Set<String> inputs) {
        this.inputs = inputs;
    }

    @Override
    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    @Override
    public String toString() {
        return "OperatorImpl{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", inputs=" + inputs +
                ", props=" + props +
                '}';
    }
}
