package spark.streaming.receiver.kafka;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import scala.Tuple2;

import com.google.common.collect.ImmutableMap;

/**
 * 
 * Kafka ETL Example Test Case.
 * 
 * It is demonstrated that the messages consumed from kafka using Spark Kafka Receiver written by Kafka Simple Consumer API are saved as avro files onto hdfs.
 * 
 *
 */

public class KafkaSimpleEtlTestSkip {	
	
	@Before
	public void init() throws Exception
	{	
		java.net.URL url = new KafkaSimpleEtlTestSkip().getClass().getResource("/log4j-test.xml");
		System.out.println("log4j url: " + url.toString());
		DOMConfigurator.configure(url);
	}
	
	

	@Test
	public void run() throws Exception {
		
		// kafka broker host list.
		String brokers = "spark005-dev.mykidong.com,spark006-dev.mykidong.com";
		String[] brokerTokens = brokers.split(",");
		List<String> brokerList = Arrays.asList(brokerTokens);
		
		// kafka broker port.
		int brokerPort = 9092;
		
		String zookeeperQuorumList = "spark003-dev.mykidong.com:2181,spark004-dev.mykidong.com:2181,spark005-dev.mykidong.com:2181";
		
		// znode base path.
		String zookeeperBasePath = "/kafka-simple-etl";
		
		// topic list.
		String topics = "item-view-event,cart-event,order-event,relevance-event,impression-event";		
		String[] topicTokens = topics.split(",");
		
		// partition count per topic.
		int partitionCount = 2;
		
		// kafka client id.
		String clientId = "spark-kafka-simple-receiver";
		
		// kafka message fetch size.
		int fetchSizeBytes = 800000;	
		
		// micro batch cycle duration.
		long duration = 20000;
		
		// avro output base path.
		String outputPathBase = "target/kafka-simple-etl";		
		

		SparkConf sparkConf = new SparkConf();		
		sparkConf.setMaster("local[50]");	
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");	
		sparkConf.set("spark.streaming.blockInterval", "200");
		sparkConf.setAppName("KafkaSimpleEtlTestSkip");	
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/HH");
		String dateFormatted = sdf.format(new Date());		
		
	
		// first, delete all the directories in which no _SUCCESS file exists, that is, it is assumed that the spark etl job failed while saving avro onto hdfs.
		FileSystem fs = FileSystem.get(ctx.hadoopConfiguration());
		
		for(String eventType : topicTokens)
		{
			String outPath = outputPathBase + "/" + eventType + "/hourly/" + dateFormatted + "/" ;
			
			Path outputPath = new Path(outPath);
			if(fs.exists(outputPath))
			{
				for(FileStatus status : fs.listStatus(new Path(outPath)))
				{
					// uuid directory.
					if(status != null && status.isDirectory())
					{
						boolean success = false;
						for(FileStatus subStatus : fs.listStatus(status.getPath()))
						{
							if(subStatus.isFile())
							{
								if(subStatus.getPath().getName().endsWith("_SUCCESS"))
								{
									success = true;	
									
									break;
								}
							}
						}						
					
						if(!success)
						{
							fs.delete(status.getPath(), true);
							System.out.println("Failure Path: [" + status.getPath().toString() + "] is deleted!!!");
						}
						else
						{
							System.out.println("Success Path: [" + status.getPath().toString() + "] not deleted!!!");
						}
					}
				}	
			}
		}
		
		Configuration hadoopConf = ctx.hadoopConfiguration();
		Map<String, String> confMap = new HashMap<String, String>();		
		Iterator<Entry<String, String>> iter = hadoopConf.iterator();
		while(iter.hasNext())
		{
			Entry<String, String> entry = iter.next();
			confMap.put(entry.getKey(), entry.getValue());
		}	
		
	
		JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(duration));
		
		JavaDStream<EventStream> unionStreams = KafkaReceiverUtils.createStream(ssc, 
																				topicTokens, 
																				partitionCount, 
																				zookeeperBasePath, 
																				zookeeperQuorumList, 
																				brokerList, 
																				brokerPort, 
																				clientId, 
																				fetchSizeBytes);
	
		// key(eventType), EventStream.
		JavaPairDStream<String, EventStream> eventTypeEventStream = unionStreams.mapToPair(new EventTypePair());			
	
		
		for(String eventType : topicTokens)
	    {	    
			// filtering by eventType
	    	JavaPairDStream<String, EventStream> filteredEventStream = eventTypeEventStream.filter(new FilterEvent(eventType));	  
	    
	    	// save messages as avro onto hdfs.
	    	filteredEventStream.foreachRDD(new SaveAvroToHdfs(eventType, outputPathBase, dateFormatted, confMap));
	    }		
		
		ssc.start();
		ssc.awaitTermination();
	}
	
	public static class EventTypePair implements PairFunction<EventStream, String, EventStream>
	{		
		@Override
		public Tuple2<String, EventStream> call(EventStream t)
				throws Exception {
			
			byte[] avroBytes = t.getPayload();
			
			try {					
				Schema.Parser parser = new Schema.Parser();
				Schema schema = parser.parse(getClass().getResourceAsStream(
						"/META-INF/avro/event-list.avsc"));
				
				// avro decoding.
				DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
						schema);
				Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes,
						null);							
				
				GenericRecord records = reader.read(null, decoder);
				
				String eventType = ((Utf8) records.get("eventType")).toString();	
			
				return new Tuple2(eventType, t);
											
			} catch (IOException e) {
				throw new RuntimeException(e.getMessage());
			}				
		}
	}
	
	public static class FilterEvent implements Function<Tuple2<String, EventStream>, Boolean>
	{
		private String eventType;
		
		public FilterEvent(String eventType)
		{
			this.eventType = eventType;
		}
		
		@Override
		public Boolean call(Tuple2<String, EventStream> t) throws Exception {		
			return t._1.equals(eventType);
		}
	}
	
	public static class SaveAvroToHdfs implements Function2<JavaPairRDD<String, EventStream>, Time, Void>
	{
		private String eventType;
		private String outputPath;
		private String dateFormatted;
		private Map<String, String> confMap;
		
		public SaveAvroToHdfs(String eventType, String outputPath, String dateFormatted, Map<String, String> confMap)
		{
			this.outputPath = outputPath;
			this.eventType = eventType;
			this.dateFormatted = dateFormatted;
			this.confMap = confMap;
		}
		
		@Override
		public Void call(JavaPairRDD<String, EventStream> pairRdd, Time time)
				throws Exception {	
			
			// when rdd count is 0, just return.
			if(pairRdd.count() == 0)
			{
				return null;
			}
			
			System.out.println("pairRdd count: [" + pairRdd.count() + "]");
		
			// avro record rdd.
			JavaPairRDD<AvroWrapper<GenericRecord>, NullWritable> avroRdd = pairRdd.mapToPair(new ToGenericRecord());	
			
			System.out.println("avroRdd count: [" + avroRdd.count() + "]");	
			
			JobConf conf = new JobConf();
			conf.set("avro.output.schema", getSchemaString());
			conf.set("avro.output.codec", "snappy");				
			
			// out path format is yyyy/MM/dd/HH/<uuid>.
			// /polaris/kafka-etl/destination/relevance-event/hourly
			String outPath = this.outputPath + "/" + eventType + "/hourly/" + dateFormatted + "/" + UUID.randomUUID().toString();
		
			// save avro to hdfs.
			avroRdd.saveAsHadoopFile(outPath, new AvroWrapper<GenericRecord>().getClass(), NullWritable.class, AvroOutputFormat.class, conf);						
		
			// build hadoop configuration from map.
			Configuration hadoopConf = new Configuration();
			for(String key : this.confMap.keySet())
			{
				hadoopConf.set(key, this.confMap.get(key));
			}
			
			FileSystem fs = FileSystem.get(hadoopConf);
			boolean successFileExists = false;
			
			int maxTry = 5;
			while(maxTry > 0)
			{
				if(fs.exists(new Path(outPath)))
				{
					
					for(FileStatus status : fs.listStatus(new Path(outPath)))
					{
						if(status.isFile())
						{
							// success file exists.
							if(status.getPath().toString().endsWith("_SUCCESS"))
							{
								successFileExists = true;
								
								System.out.println("_SUCCESS file found: [" + status.getPath().toString() + "]");
								
								break;
							}
						}
					}
				}
				
				if(successFileExists)
				{
					break;
				}
				
				Thread.sleep(1000);
				maxTry--;
			}
			
			if(successFileExists)
			{				
				// update process offset to znode path.
				pairRdd.mapToPair(new EventStreamPair()).groupByKey().mapPartitionsToPair(new MaxOffsetEventStream()).foreach(new UpdateMaxOffset());	
			}
			
			
			return null;
		}
		
		private String getSchemaString()
		{		    
		    Schema.Parser parser = new Schema.Parser();
			try {
				Schema schema = parser.parse(getClass().getResourceAsStream(
						"/META-INF/avro/event-list.avsc"));						
				
				return schema.toString();
			} catch (IOException e) {			
				e.printStackTrace();
			}
			
			return null;
		}
		
		public static class UpdateMaxOffset implements VoidFunction<Tuple2<String, EventStream>>
		{			
			@Override
			public void call(Tuple2<String, EventStream> t)
					throws Exception {
			
				// update process offset onto znode.
				EventStream maxEventStream = t._2;		
				
				long currentProcessOffset = maxEventStream.getOffset();
				
				Map<Object, Object> data = (Map<Object, Object>) ImmutableMap
						.builder()
						.put("consumer", ImmutableMap.of("id", maxEventStream.getClientId()))
						.put("offset", currentProcessOffset)
						.put("partition", maxEventStream.getPartition())
						.put("broker", ImmutableMap.of("host", maxEventStream.getBrokerHost(), "port", maxEventStream.getBrokerPort()))
						.put("topic", maxEventStream.getTopic()).build();
				
				String processPath = KafkaSimpleConsumer.buildProcessPath(maxEventStream.getZookeeperBasePath(), maxEventStream.getClientId(), maxEventStream.getTopic(), maxEventStream.getPartition());
				
				ZookeeperState zookeeperState = new ZookeeperState(maxEventStream.getZookeeperQuorumList());
			
				// read old process offset from zk.
				Map<Object, Object> processData = zookeeperState.readJSON(processPath);		
				long oldProcessOffset = 0;
				if(processData != null)
				{
					oldProcessOffset = (Long) processData.get("offset");				
				}	
			
				// if current process offset is greater than the old process offset, update the current one onto znode.
				if(oldProcessOffset < currentProcessOffset)
				{						
					zookeeperState.writeTransactionalJSON(processPath, data);
					System.out.println("Wrote processed offset to zk path: [" + processPath + "], data: [" + new ObjectMapper().writeValueAsString(data) + "]");	
				}
				else
				{
					System.out.println("old process offset [" + oldProcessOffset + "] is greater than current process offset [" + currentProcessOffset + "] ...");	
				}
			}
		}
		
		public static class MaxOffsetEventStream implements PairFlatMapFunction<Iterator<Tuple2<String, Iterable<EventStream>>>, String, EventStream>
		{
			@Override
			public Iterable<Tuple2<String, EventStream>> call(
					Iterator<Tuple2<String, Iterable<EventStream>>> iter)
					throws Exception {	
				List<Tuple2<String, EventStream>> tupleList = new ArrayList<Tuple2<String, EventStream>>();
				
				while(iter.hasNext())
				{
					Tuple2<String, Iterable<EventStream>> t = iter.next();
					String key = t._1;
					
					Iterator<EventStream> innerIter = t._2.iterator();					
					List<EventStream> esList = new ArrayList<EventStream>();				
					while(innerIter.hasNext())
					{
						EventStream es = innerIter.next();							
						esList.add(es);					
					}
					
					// offset DESC Sorting.
					Collections.sort(esList, new Comparator<EventStream>() {
						@Override
						public int compare(EventStream es1, EventStream es2) {						
							return (int)(es2.getOffset() - es1.getOffset());
						}});			
				
					// select just the entry having max. offset.
					tupleList.add(new Tuple2(key, esList.get(0)));					
				}						
				
				return tupleList;
			}
		}
		
		public static class EventStreamPair implements PairFunction<Tuple2<String, EventStream>, String, EventStream>
		{
			@Override
			public Tuple2<String, EventStream> call(
					Tuple2<String, EventStream> t) throws Exception {				
				
				String eventType = t._1;
				
				EventStream es = t._2;
				
				String clientId = es.getClientId();
				String topic = es.getTopic();
				int partition = es.getPartition();
				
				
				return new Tuple2(eventType + ":" + clientId + ":" + topic + ":" + partition, es);
			}
		}
		
		public static class ToGenericRecord implements PairFunction<Tuple2<String, EventStream>, AvroWrapper<GenericRecord>, NullWritable>
		{
			@Override
			public Tuple2<AvroWrapper<GenericRecord>, NullWritable> call(
					Tuple2<String, EventStream> t) throws Exception {
				
				byte[] avroBytes = t._2.getPayload();						
			
				try {					
					Schema.Parser parser = new Schema.Parser();
					Schema schema = parser.parse(getClass().getResourceAsStream(
							"/META-INF/avro/event-list.avsc"));
					
					// avro decoding.
					DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
							schema);
					Decoder decoder = DecoderFactory.get().binaryDecoder(avroBytes,
							null);							
					
					GenericRecord records = reader.read(null, decoder);					
					
					return new Tuple2(new AvroWrapper(records), NullWritable.get());							
				} catch (IOException e) {
					throw new RuntimeException(e.getMessage());
				}				
			}
		}
	}
	
	@After
	public void shutdown()
	{
		
	}

}
