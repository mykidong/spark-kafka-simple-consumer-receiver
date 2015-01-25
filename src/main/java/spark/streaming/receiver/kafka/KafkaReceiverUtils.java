package spark.streaming.receiver.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class KafkaReceiverUtils {
	
	public static JavaDStream<EventStream> createStream(JavaStreamingContext ssc, 
														String topics, 
														int partitionCount,
														String zookeeperBasePath, 
														String zookeeperQuorumList, 
														String brokers,
														int brokerPort, 
														String clientId,
														int fetchSizeBytes)
	{
		String[] brokerTokens = brokers.split(",");
		List<String> brokerList = Arrays.asList(brokerTokens);
		
		String[] topicTokens = topics.split(",");		
		
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
		
		return unionStreams;
	}	

}
