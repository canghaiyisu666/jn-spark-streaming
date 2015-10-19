package com.scistor.spark.streaming.operator.common;

import org.apache.spark.streaming.api.java.JavaDStream;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author Wei Xing
 */
public interface IWriter extends Serializable {

    public void write(SchemaDStream input, Properties props) throws Exception;
}
