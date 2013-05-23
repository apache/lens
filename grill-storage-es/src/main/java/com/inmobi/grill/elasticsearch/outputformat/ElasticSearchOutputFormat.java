package com.inmobi.grill.elasticsearch.outputformat;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.inmobi.grill.elasticsearch.Document;
import com.inmobi.grill.elasticsearch.ElasticSearchIndexer;

/**
 * Hadoop OutputFormat for writing arbitrary MapWritables (essentially HashMaps)
 * into Elasticsearch. Records are batched up and sent in a one-hop manner to
 * the elastic search data nodes that will index them.
 */
public class ElasticSearchOutputFormat implements Configurable,
		HiveOutputFormat<NullWritable, MapWritable> {

	public static final String ES_INDEX_NAME = "elasticsearch.index.name";
	public static final String TABLE_NAME = "hive.table.name";
	public static final String ES_CLUSTER_NAME = "elasticsearch.cluster.name";
	public static final String ES_HOST_NAME = "elasticsearch.server.host";
	public static final String ES_HOST_PORT = "elasticsearch.server.port";
	public static final String ES_INDEX_BATCH_SIZE = "elasticsearch.index.batchSize";
	public static final String ES_PARTITION_NAME = "elasticsearch.index.partition";

	public static final int ES_BATCH_SIZE = 1000;

	public static final String ES_REFRESH_AFTER_BATCH = "elasticsearch.refresh.after.batch";

	public static final String ES_OVERWRITE_ON_DUP_ID = "elasticsearch.overwrite.on.duplicate.id";

	private Configuration conf = null;


	protected class ElasticSearchRecordWriter implements RecordWriter {

		private ArrayList<Document> currentBatch;
		private int batchSize;
		private com.inmobi.grill.elasticsearch.ElasticSearchIndexer indexer;
		private String indexName;
		private String esCluster;
		private String esHost;
		private int esPort;
		private boolean refreshAfterBatch;
		private boolean overwrite;
		private String partitionName;
		private String taskPartition;

		private int count;

		public ElasticSearchRecordWriter(Configuration conf) {

			taskPartition = conf.get("mapred.task.partition");
			indexName = conf.get(ES_INDEX_NAME);
			System.out.println("indexName >>>>>" + indexName);
			checkNotNull(indexName);
			conf.addResource(getClass().getClassLoader().getResourceAsStream("es-site.xml"));
			esCluster = conf.get(ES_CLUSTER_NAME,
					ElasticSearchIndexer.ES_DEFAULT_CLUSTER);
			esHost = conf.get(ES_HOST_NAME, "localhost");
			esPort = conf.getInt(ES_HOST_PORT,
					ElasticSearchIndexer.ES_DEFAULT_PORT);
			batchSize = conf.getInt(ES_INDEX_BATCH_SIZE, ES_BATCH_SIZE);
			currentBatch = new ArrayList<Document>(batchSize);
			indexer = new ElasticSearchIndexer(esCluster, esHost, esPort);
			refreshAfterBatch = conf.getBoolean(ES_REFRESH_AFTER_BATCH, false);
			overwrite = conf.getBoolean(ES_OVERWRITE_ON_DUP_ID, true);
			partitionName = conf.get(ES_PARTITION_NAME, "");

			checkNotNull(partitionName);
			System.out.println("Starting with esCluster:$$$$$$$$$$$$$"
					+ esCluster + " esHost:" + esHost + "esPort:" + esPort +"indexName:"+indexName +"partitionName:"+partitionName);

		}

		public void connect() throws IOException {
			indexer.connect();
		}

		private String getUniqueId() {
			return this.partitionName + "." + this.taskPartition + "."
					+ Integer.toHexString(count++);
		}

		private Document makeEvent(Writable value) throws IOException {

			Document e = new Document();

			// TODO use the id in the current data
			/*
			 * if (value instanceof MapWritable) {
			 * 
			 * MapWritable o = (MapWritable) value; Writable removedId =
			 * o.get(new Text("id")); //in case there is no id in the current
			 * data if(removedId !=null){ e.setId(removedId.toString()); }
			 * 
			 * }
			 */

			e.setId(this.getUniqueId());
			XContentBuilder docBuilder = XContentFactory.jsonBuilder();
			buildContent(docBuilder, value);
			e.setDocument(docBuilder);
			return e;

		}

		@Override
		public void write(Writable value) throws IOException {

			Document evt = makeEvent(value);

			synchronized (this) {
				if (shouldIndex()) {
					indexBatch(currentBatch);
				}
			}
			// Write this event to current buffer
			currentBatch.add(evt);
		}

		

		// Flush current event buffer to elasticsearch, and clear the buffer
		private void indexBatch(ArrayList<Document> batch) throws IOException {

			try {

				String physicalIndex = indexName;
				System.out.println("IndexName ++++++" + physicalIndex);
				indexer.indexBatch(physicalIndex, batch.iterator(),
						refreshAfterBatch, overwrite);
				batch.clear();
			} catch (IOException ioerr) {
				// LOG.warn("Batch failed:" + ioerr.getMessage(), ioerr);
				throw ioerr;
			}
		}

		private boolean shouldIndex() {
			return currentBatch.size() >= batchSize;
		}

		@Override
		public void close(boolean arg0) throws IOException {
			// Check if batch has pending items, index them if necessary
			// Then close the indexer.
			try {
				if (!currentBatch.isEmpty()) {
					// LOG.info(">> Indexing leftover " + currentBatch.size());
					indexBatch(currentBatch);
				}
			} catch (IOException ioerr) {
				// LOG.error("Error sending final batch to elasticsearch - " +
				// ioerr.getMessage(), ioerr);
				throw ioerr;
			} finally {
				indexer.close();
			}
		}

		/**
		 * Recursively untangles the MapWritable and writes the fields into
		 * elasticsearch's XContentBuilder builder.
		 */
		private void buildContent(XContentBuilder builder, Writable value)
				throws IOException {

			if (value instanceof Text) {
				builder.value(((Text) value).toString());
			} else if (value instanceof LongWritable) {
				builder.value(((LongWritable) value).get());
			} else if (value instanceof IntWritable) {
				builder.value(((IntWritable) value).get());
			} else if (value instanceof DoubleWritable) {
				builder.value(((DoubleWritable) value).get());
			} else if (value instanceof FloatWritable) {
				builder.value(((FloatWritable) value).get());
			} else if (value instanceof BooleanWritable) {
				builder.value(((BooleanWritable) value).get());
			} else if (value instanceof org.apache.hadoop.hive.serde2.io.DoubleWritable) {
				builder.value(((org.apache.hadoop.hive.serde2.io.DoubleWritable) value)
						.get());
			} else if (value instanceof MapWritable) {
				builder.startObject();
				for (Map.Entry<Writable, Writable> entry : ((MapWritable) value)
						.entrySet()) {
					if (!(entry.getValue() instanceof NullWritable)) {
						builder.field(entry.getKey().toString());
						buildContent(builder, entry.getValue());
					}
				}
				builder.endObject();
			} else if (value instanceof ArrayWritable) {
				builder.startArray();
				Writable[] arrayOfThings = ((ArrayWritable) value).get();
				for (int i = 0; i < arrayOfThings.length; i++) {
					buildContent(builder, arrayOfThings[i]);
				}
				builder.endArray();
			}
		}

	}

	public void setConf(Configuration conf) {
	}

	public Configuration getConf() {
		return conf;
	}

	@Override
	public RecordWriter getHiveRecordWriter(JobConf jobconf, Path path,
			Class<? extends Writable> class1, boolean flag,
			Properties properties, Progressable progressable)
			throws IOException {
		ElasticSearchRecordWriter returnElasticSearchRecordWriter = new ElasticSearchRecordWriter(
				jobconf);
		returnElasticSearchRecordWriter.connect();

		return returnElasticSearchRecordWriter;
	}



	@Override
	public void checkOutputSpecs(FileSystem arg0, JobConf arg1)
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable> getRecordWriter(
			FileSystem arg0, JobConf arg1, String arg2, Progressable arg3)
			throws IOException {
		throw new RuntimeException("Error: Hive should not invoke this method.");
	}
}
