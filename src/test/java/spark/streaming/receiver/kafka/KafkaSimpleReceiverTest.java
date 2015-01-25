package spark.streaming.receiver.kafka;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.xml.DOMConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * Kafka Simple Receiver Test Case.
 * 
 * 
 *
 */

public class KafkaSimpleReceiverTest {	
	
	@Before
	public void init() throws Exception
	{	
		java.net.URL url = new KafkaSimpleReceiverTest().getClass().getResource("/log4j-test.xml");
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
		String zookeeperBasePath = "/kafka-simple-receiver";
		
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
		
	
		SparkConf sparkConf = new SparkConf();		
		sparkConf.setMaster("local[50]");	
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");	
		sparkConf.set("spark.streaming.blockInterval", "200");
		sparkConf.setAppName("KafkaSimpleReceiverTest");	
		
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);		
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
		
		// DO SOMETHING, HERE!!!
		
		unionStreams.print();		
			
		
		ssc.start();
		ssc.awaitTermination();
	}
	
	
	
	@After
	public void shutdown()
	{
		
	}

}
