package com.scistor.spark.streaming.operator;

import com.scistor.spark.streaming.operator.common.IWriter;
import com.scistor.spark.streaming.operator.common.SchemaDStream;
import com.scistor.spark.streaming.operator.common.SerHCatRecord;

import iie.udps.common.hcatalog.SerHCatOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.spark.SerializableWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.thrift.TException;

import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class HiveWriter implements IWriter {
	@Override
	public void write(SchemaDStream input, Properties props) throws Exception {
		String dbName = props.getProperty("DbName");// 解析输出参数
		String tableName = props.getProperty("TableName");

		boolean flag = tableExist(dbName, tableName);
		System.out.println("flag===============" + flag);
		if (!flag) {// 不存在就先建表
			HiveMetaStoreClient client = null;
			try {
				HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
				try {
					client = HCatUtil.getHiveClient(hiveConf);
				} catch (MetaException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			List<FieldSchema> fields = HCatUtil.getFieldSchemaList(input
					.schema().getFields());
			System.out.println(fields);
			Table table = new Table();
			table.setDbName(dbName);
			table.setTableName(tableName);

			StorageDescriptor sd = new StorageDescriptor();
			sd.setCols(fields);
			table.setSd(sd);
			sd.setInputFormat(RCFileInputFormat.class.getName());
			sd.setOutputFormat(RCFileOutputFormat.class.getName());
			sd.setParameters(new HashMap<String, String>());
			sd.setSerdeInfo(new SerDeInfo());
			sd.getSerdeInfo().setName(table.getTableName());
			sd.getSerdeInfo().setParameters(new HashMap<String, String>());
			sd.getSerdeInfo().getParameters()
					.put(serdeConstants.SERIALIZATION_FORMAT, "1");
			sd.getSerdeInfo().setSerializationLib(
					org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe.class
							.getName());
			Map<String, String> tableParams = new HashMap<String, String>();
			table.setParameters(tableParams);
			try {
				client.createTable(table);
				System.out.println("==========Create table successfully!");
			} catch (TException e) {
				e.printStackTrace();
				return;
			} finally {
				client.close();
			}
		}

		Job outputJob = null;
		outputJob = Job.getInstance();
		outputJob.setJobName("HiveWriterJob+" + dbName + "+" + tableName);
		outputJob.setOutputKeyClass(WritableComparable.class);
		outputJob.setOutputValueClass(SerializableWritable.class);
		SerHCatOutputFormat.setOutput(outputJob,
				OutputJobInfo.create(dbName, tableName, null));
		HCatSchema schema = input.schema();
		SerHCatOutputFormat.setSchema(outputJob, schema);
		outputJob.setOutputFormatClass(SerHCatOutputFormat.class);

		input.dstream()
				.mapToPair(
						new PairFunction<SerHCatRecord, WritableComparable, SerializableWritable<HCatRecord>>() {

							@Override
							public Tuple2<WritableComparable, SerializableWritable<HCatRecord>> call(
									SerHCatRecord record) throws Exception {
								return new Tuple2<WritableComparable, SerializableWritable<HCatRecord>>(
										NullWritable.get(),
										new SerializableWritable<HCatRecord>(
												record));
							}

						})
				.foreachRDD(
						new WriteToHive(
								new SerializableWritable<Configuration>(
										outputJob.getConfiguration())));
	}

	private boolean tableExist(String dbName, String tableName) {
		HiveMetaStoreClient client = null;
		boolean result = true;
		try {
			HiveConf hiveConf = HCatUtil.getHiveConf(new Configuration());
			client = HCatUtil.getHiveClient(hiveConf);
			result = client.tableExists(dbName, tableName);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return result;
	}

	public static class WriteToHive
			implements
			Function<JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>>, Void> {

		private SerializableWritable<Configuration> conf;

		public WriteToHive(SerializableWritable<Configuration> conf) {
			this.conf = conf;
		}

		@Override
		public Void call(
				JavaPairRDD<WritableComparable, SerializableWritable<HCatRecord>> rdd)
				throws Exception {
			rdd.saveAsNewAPIHadoopDataset(conf.value());
			return null;
		}
	}
}
