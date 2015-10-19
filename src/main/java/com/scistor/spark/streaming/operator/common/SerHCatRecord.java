package com.scistor.spark.streaming.operator.common;

import org.apache.hive.hcatalog.data.DefaultHCatRecord;

import java.io.Serializable;
import java.util.List;

/**
 * @author Wei Xing
 */
public class SerHCatRecord extends DefaultHCatRecord implements Serializable {

    public SerHCatRecord() {
    }

    public SerHCatRecord(int size) {
        super(size);
    }

    public SerHCatRecord(List<Object> list) {
        super(list);
    }
}
