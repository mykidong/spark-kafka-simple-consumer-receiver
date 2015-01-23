package mykidong.spark.receiver;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

public class KafkaSimpleConsumer implements Runnable {
	
	public static final Logger log = LoggerFactory.getLogger(KafkaSimpleConsumer.class);

	private KafkaSimpleReceiver kafkaSimpleReceiver;
	private KafkaConfiguration kafkaConfiguration;
	private List<String> replicaBrokers = new ArrayList<String>();
	private ZookeeperState zookeeperState;

	public KafkaSimpleConsumer(KafkaSimpleReceiver kafkaSimpleReceiver, KafkaConfiguration kafkaConfiguration) {
		this.kafkaSimpleReceiver = kafkaSimpleReceiver;
		this.kafkaConfiguration = kafkaConfiguration;
		
		this.zookeeperState = new ZookeeperState(this.kafkaConfiguration.getZookeeperQuorumList());
	}

	@Override
	public void run() {

		// find the meta data about the topic and partition we are interested in
		//
		PartitionMetadata metadata = findLeader(this.kafkaConfiguration.getSeedBrokerList(), 
										        this.kafkaConfiguration.getBrokerPort(), 
										        this.kafkaConfiguration.getTopic(),
										        this.kafkaConfiguration.getPartition());
		if (metadata == null) {
			log.info("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			log.info("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		
		
		String leadBroker = metadata.leader().host();
		int leadBrokerPort = metadata.leader().port();
		String clientName = this.kafkaConfiguration.getClientId();

		SimpleConsumer consumer = new SimpleConsumer(leadBroker, 
													 this.kafkaConfiguration.getBrokerPort(),
													 100000, 
													 64 * 1024, 
													 clientName);
		
		long readOffset = 0L;
		
		// read the offset from commit path in zookeeper.
		String commitPath = this.getCommitPath();
		Map<Object, Object> commitData = this.zookeeperState.readJSON(commitPath);
		log.info("Read partition information from: " + commitPath + " [" + commitData + "]");		
		
		if (commitData != null) {		
			long commitOffset = (Long) commitData.get("offset");
			
			readOffset = commitOffset;
			
			// read the offset from process path in zookeeper
			String processPath = this.getProcessPath();
			Map<Object, Object> processData = this.zookeeperState.readJSON(processPath);
			log.info("Read partition information from: " + processPath + " [" + processData + "]");	
			
			if(processData != null)
			{
				long processOffset = (Long) processData.get("offset");
				
				// if process offset is less than commit offset, then set process offset to read offset.
				if(processOffset < commitOffset)
				{
					readOffset = processOffset;
					log.info("processOffset [" + processOffset + "] is less than commitOffset [" + commitOffset + "], set processOffset [" + processOffset + "] to start readOffset...");
				}
			}			
		}
		else
		{		
			readOffset = getLastOffset(consumer, 
										this.kafkaConfiguration.getTopic(), 
										this.kafkaConfiguration.getPartition(),
										kafka.api.OffsetRequest.EarliestTime(), 
										clientName);			
		}
		
		
		log.info("start readOffset: [" + readOffset + "], topic: [" + this.kafkaConfiguration.getTopic() + "], partition: [" + this.kafkaConfiguration.getPartition() + "]");
	
		int numErrors = 0;
	
		while (true) 
		{
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, 
											  this.kafkaConfiguration.getBrokerPort(), 
						                      100000,
						                      64 * 1024, 
						                      clientName);
			}
			
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(this.kafkaConfiguration.getTopic(), this.kafkaConfiguration.getPartition(), readOffset, this.kafkaConfiguration.getFetchSizeBytes()) 
					.build();
			FetchResponse fetchResponse = consumer.fetch(req);

			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(this.kafkaConfiguration.getTopic(), this.kafkaConfiguration.getPartition());
				log.info("Error fetching data from the Broker:"
						+ leadBroker + " Reason: " + code);
				if (numErrors > 5)
					break;
				
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for
					// the last element to reset
					readOffset = getLastOffset(consumer, 
											   this.kafkaConfiguration.getTopic(), 
											   this.kafkaConfiguration.getPartition(),
							                   kafka.api.OffsetRequest.LatestTime(), 
							                   clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				try {
					leadBroker = findNewLeader(leadBroker, 
											   this.kafkaConfiguration.getTopic(), 
											   this.kafkaConfiguration.getPartition(),
							                   this.kafkaConfiguration.getBrokerPort());
				} catch (Exception e) {					
					e.printStackTrace();
				}
				
				continue;
			}
			
			numErrors = 0;
			
			long commitOffset = 0;

			List<EventStream> eventLogList = new ArrayList<EventStream>();			
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(this.kafkaConfiguration.getTopic(), this.kafkaConfiguration.getPartition())) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					log.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				
				if(commitOffset < currentOffset)
				{
					commitOffset = currentOffset;
				}
			
				readOffset = messageAndOffset.nextOffset();
								
				Message message = messageAndOffset.message();
				ByteBuffer payload = message.payload();

				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);					
		
				EventStream eventLog = new EventStream();
				eventLog.setPayload(bytes);
				eventLog.setClientId(this.kafkaConfiguration.getClientId());
				eventLog.setOffset(currentOffset);
				eventLog.setPartition(this.kafkaConfiguration.getPartition());
				eventLog.setTopic(this.kafkaConfiguration.getTopic());
				eventLog.setBrokerHost(leadBroker);
				eventLog.setBrokerPort(leadBrokerPort);
				eventLog.setZookeeperQuorumList(this.kafkaConfiguration.getZookeeperQuorumList());
				eventLog.setZookeeperBasePath(this.kafkaConfiguration.getZookeeperBasePath());
				if(message.hasKey())
				{
					eventLog.setKey(message.key().array());
				}
				
				eventLogList.add(eventLog);						
			}

			if (eventLogList.size() == 0) {				
				try {
					Thread.sleep(1000);			
				} catch (InterruptedException ie) {
				}
			}
			else
			{
				// store event log to spark streaming block store.
				this.kafkaSimpleReceiver.store(eventLogList.iterator());
			
				// write offset to zookeeper.
				commit(leadBroker, leadBrokerPort, commitOffset);
				
				eventLogList.clear();
				eventLogList = null;
			}
		}	
	}
	
	
	public void commit(String leadBrokerHost, int leadBrokerPort, long readOffset) {
		
		Map<Object, Object> data = (Map<Object, Object>) ImmutableMap
				.builder()
				.put("consumer", ImmutableMap.of("id", this.kafkaConfiguration.getClientId()))
				.put("offset", readOffset)
				.put("partition", this.kafkaConfiguration.getPartition())
				.put("broker", ImmutableMap.of("host", leadBrokerHost, "port", leadBrokerPort))
				.put("topic", this.kafkaConfiguration.getTopic()).build();
		
		this.zookeeperState.writeJSON(this.getCommitPath(), data);
		log.info("Wrote committed offset to ZK: " + readOffset);		
	}

	private String getCommitPath() {
		return this.kafkaConfiguration.getZookeeperBasePath() + "/" + this.kafkaConfiguration.getClientId() + "/" + this.kafkaConfiguration.getTopic() + "/partition-" + this.kafkaConfiguration.getPartition(); 
	}
	
	private String getProcessPath() {
		return buildProcessPath(this.kafkaConfiguration.getZookeeperBasePath(), this.kafkaConfiguration.getClientId(), this.kafkaConfiguration.getTopic(), this.kafkaConfiguration.getPartition()); 
	}
	
	public static String buildProcessPath(String zookeeperBasePath, String clientId, String topic, int partition)
	{
		StringBuffer sb = new StringBuffer();
		sb.append(zookeeperBasePath).append("/");
		sb.append(clientId).append("/");
		sb.append(topic).append("/process/partition-");
		sb.append(partition);
		
		return sb.toString();
	}

	private long getLastOffset(SimpleConsumer consumer, 
							   String topic,
							   int partition, 
							   long whichTime, 
							   String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
				clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			log.info("Error fetching data Offset Data the Broker. Reason: "
							+ response.errorCode(topic, partition));
			return 0;
		}
		long[] offsets = response.offsets(topic, partition);
		
		return offsets[0];
	}

	private String findNewLeader(String oldLeader, 
								 String topic,
			                     int partition, 
			                     int port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(replicaBrokers, port,
					topic, partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (oldLeader.equalsIgnoreCase(metadata.leader().host())
					&& i == 0) {
				// first time through if the leader hasn't changed give
				// ZooKeeper a second to recover
				// second time, assume the broker did recover before failover,
				// or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		log.info("Unable to find new leader after Broker failure. Exiting");

		throw new Exception(
				"Unable to find new leader after Broker failure. Exiting");
	}

	private PartitionMetadata findLeader(List<String> seedBrokers,
			int port, String topic, int partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seed : seedBrokers) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024,
						this.kafkaConfiguration.getClientId());
				List<String> topics = Collections.singletonList(topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				log.info("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + topic + ", "
						+ partition + "] Reason: " + e);
			} finally {
				if (consumer != null)
					consumer.close();
			}
		}
		if (returnMetaData != null) {
			replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}
	
	public void stop()
	{
		this.zookeeperState.close();
	}

}
