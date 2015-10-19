package com.scistor.spark.streaming;

import com.scistor.spark.streaming.workflow.Workflow;
import com.scistor.spark.streaming.workflow.json.WorkflowFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author Wei Xing
 */
public class Application {

    public static void main(String[] args) throws Exception {
        // TODO process arguments.
        String filePath = args[0];

        String definition = IOUtils.toString(new FileInputStream(new File(filePath)));
        Workflow workflow = new WorkflowFactory().getWorkflow(definition);

        JavaStreamingContext jsc = new JavaStreamingContext(new SparkConf(), Seconds.apply(10));
        new AppEngine(jsc, workflow).setUp();
        jsc.start();
        jsc.awaitTermination();
    }
}
