package com.scistor.spark.streaming.operator.common;

import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.streaming.api.java.JavaDStream;

/**
 * @author Wei Xing
 */
public class SchemaDStream {

    private JavaDStream<SerHCatRecord> dstream;
    private HCatSchema schema;

    public SchemaDStream(JavaDStream<SerHCatRecord> dstream, HCatSchema schema) {
        this.dstream = dstream;
        this.schema = schema;
    }

    public JavaDStream<SerHCatRecord> dstream() {
        return dstream;
    }

    public HCatSchema schema() {
        return schema;
    }
}
