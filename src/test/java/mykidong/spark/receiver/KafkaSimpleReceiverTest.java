package mykidong.spark.receiver;

import java.util.ArrayList;
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
		String brokers = "polaris005-dev.gslook.com,polaris006-dev.gslook.com";
		String[] brokerTokens = brokers.split(",");
		List<String> brokerList = Arrays.asList(brokerTokens);
		
		// kafka broker port.
		int brokerPort = 9092;
		
		String zookeeperQuorumList = "polaris003-dev.gslook.com:2181,polaris004-dev.gslook.com:2181,polaris005-dev.gslook.com:2181";
		
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
		
		JavaDStream<EventStream> unionStreams = null;		
		List<JavaDStream<EventStream>> streamsList = new ArrayList<JavaDStream<EventStream>>();		
	
		for (int i = 0; i < partitionCount; i++) 
		{		
			for(String topic : topicTokens)
			{
				KafkaConfiguration kafkaConfiguration = new KafkaConfiguration();
				kafkaConfiguration.setTopic(topic);
				kafkaConfiguration.setPartition(i);
				kafkaConfiguration.setZookeeperBasePath(zookeeperBasePath);
				kafkaConfiguration.setZookeeperQuorumList(zookeeperQuorumList);			
				kafkaConfiguration.setSeedBrokerList(brokerList);				
				kafkaConfiguration.setBrokerPort(brokerPort);
				kafkaConfiguration.setClientId(clientId);
				kafkaConfiguration.setFetchSizeBytes(fetchSizeBytes);
				
				streamsList.add(ssc.receiverStream(new KafkaSimpleReceiver(kafkaConfiguration)));		
			}
		}	
			
		if (streamsList.size() > 1) {
			unionStreams = ssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
		} else {		
			unionStreams = streamsList.get(0);
		}		
	
		
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
