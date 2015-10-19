package com.scistor.spark.streaming.operator.common;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author Wei Xing
 */
public interface IReader extends Serializable {

    public SchemaDStream read(JavaStreamingContext jsc, Properties props) throws Exception;
}
