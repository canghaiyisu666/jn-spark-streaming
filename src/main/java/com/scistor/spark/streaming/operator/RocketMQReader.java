package com.scistor.spark.streaming.operator;

import com.alibaba.rocketmq.common.message.Message;
import com.scistor.spark.streaming.operator.common.IReader;
import com.scistor.spark.streaming.operator.common.SchemaDStream;
import com.scistor.spark.streaming.operator.common.SerHCatRecord;
import com.scistor.spark.streaming.rocketmq.RocketMQUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Wei Xing
 */
public class RocketMQReader implements IReader {
    private static String NAME_SERVER_LIST = "nameserver.list";
    private static String CONSUMER_ID = "consumer.id";
    private static String TOPIC = "topic";
    private static String TAGS = "tags";
    private static String SCHEMA = "schema";
    private static String IS_LIST = "is.list";

    @Override
    public SchemaDStream read(JavaStreamingContext jsc, Properties props) throws Exception {
        Args args = parseArgs(props);

        JavaDStream<Message> messages = RocketMQUtils.createJavaDStream(jsc.ssc(), args.nameSrvList, args.consumerId, args.topic, args.tags, StorageLevel.MEMORY_ONLY());
        ToHCatRecord toHCatRecord = new ToHCatRecord(args.schema.toString(), args.isList);
        return new SchemaDStream(messages.flatMap(toHCatRecord), toHCatSchema(args.schema));
    }

    private Args parseArgs(Properties props) {
        Args args = new Args();
        args.nameSrvList = props.getProperty(NAME_SERVER_LIST);
        args.consumerId = props.getProperty(CONSUMER_ID);
        args.topic = props.getProperty(TOPIC);
        args.tags = props.getProperty(TAGS);
        args.schema = (new Schema.Parser()).parse(props.getProperty(SCHEMA));
        args.isList = Boolean.valueOf(props.getProperty(IS_LIST));
        return args;
    }

    private static class Args {
        public String nameSrvList;
        public String consumerId;
        public String topic;
        public String tags;
        public Schema schema;
        public boolean isList;
    }

    private static class ToHCatRecord implements FlatMapFunction<Message, SerHCatRecord> {

        private String schema;
        private boolean isList;
        private transient DatumReader reader;

        public ToHCatRecord(String schema, boolean isList) {
            this.schema = schema;
            this.isList = isList;
        }

        @Override
        public Iterable<SerHCatRecord> call(Message message) throws Exception {
            Decoder decoder = DecoderFactory.get().binaryDecoder(message.getBody(), null);

            synchronized (this) {
                if (reader == null) {
                    Schema schema = new Schema.Parser().parse(this.schema);
                    if (isList) {
                        schema = Schema.createArray(schema);
                        reader = new GenericDatumReader<GenericArray>(schema);
                    } else {
                        reader = new GenericDatumReader<GenericRecord>(schema);
                    }
                }
            }

            if (isList) {
                GenericArray<GenericRecord> records = (GenericArray<GenericRecord>) reader.read(null, decoder);

                List<SerHCatRecord> result = new ArrayList<>(records.size());
                for (int i = 0; i < records.size(); i++) {
                    result.add(toHCatRecord(records.get(i)));
                }
                return result;
            } else {
                GenericRecord record = (GenericRecord) reader.read(null, decoder);

                List<SerHCatRecord> result = new ArrayList<>(1);
                result.add(toHCatRecord(record));
                return result;
            }
        }

        private SerHCatRecord toHCatRecord(GenericRecord record) {
            SerHCatRecord hCatRecord = new SerHCatRecord(record.getSchema().getFields().size());
            for (int i = 0; i < record.getSchema().getFields().size(); i++) {
                Object field = record.get(i);
                if (field instanceof Utf8) {
                    field = field.toString();
                } else if (field instanceof ByteBuffer) {
                    byte[] bytes = new byte[((ByteBuffer) field).remaining()];
                    ((ByteBuffer) field).get(bytes);
                    field = bytes;
                }
                hCatRecord.set(i, field);
            }
            return hCatRecord;
        }
    }

    public HCatSchema toHCatSchema(Schema schema) throws HCatException {
        List<HCatFieldSchema> fieldSchemas = new ArrayList<>(schema.getFields().size());
        for (Schema.Field field : schema.getFields()) {
            fieldSchemas.add(toHCatFieldSchema(field));
        }
        return new HCatSchema(fieldSchemas);
    }

    private HCatFieldSchema toHCatFieldSchema(Schema.Field field) throws HCatException {
        String name = field.name();
        PrimitiveTypeInfo type;

        switch (field.schema().getType()) {
            case BOOLEAN:
                type = TypeInfoFactory.booleanTypeInfo;
                break;
            case STRING:
                type = TypeInfoFactory.stringTypeInfo;
                break;
            case BYTES:
                type = TypeInfoFactory.binaryTypeInfo;
                break;
            case INT:
                type = TypeInfoFactory.intTypeInfo;
                break;
            case LONG:
                type = TypeInfoFactory.longTypeInfo;
                break;
            case FLOAT:
                type = TypeInfoFactory.floatTypeInfo;
                break;
            case DOUBLE:
                type = TypeInfoFactory.doubleTypeInfo;
                break;
            default:
                type = TypeInfoFactory.voidTypeInfo;
        }

        return new HCatFieldSchema(name, type, "");
    }
}
