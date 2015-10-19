package com.scistor.spark.streaming.operator;

import org.apache.avro.Schema;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Wei Xing
 */
public class RocketMQReaderTest {
    @Test
    public void testToHCatSchema() throws HCatException {
        Schema schema = new Schema.Parser().parse("{\"type\": \"record\", \"name\": \"Test\", \"fields\" : [{\"name\": \"col1\", \"type\": \"int\"}, {\"name\": \"col2\", \"type\": \"long\"}]}");
        HCatSchema hCatSchema = new RocketMQReader().toHCatSchema(schema);

        assertEquals(2, hCatSchema.size());
    }
}
