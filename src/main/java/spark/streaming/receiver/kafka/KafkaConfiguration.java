package spark.streaming.receiver.kafka;

import java.io.Serializable;
import java.util.List;

public class KafkaConfiguration implements Serializable {	
	
	private String topic;	
	private int partition;	
	private List<String> seedBrokerList;	
	private int brokerPort;	
	private String zookeeperQuorumList;		
	private String zookeeperBasePath;	
	private String clientId;
	private int fetchSizeBytes;		

	public int getFetchSizeBytes() {
		return fetchSizeBytes;
	}

	public void setFetchSizeBytes(int fetchSizeBytes) {
		this.fetchSizeBytes = fetchSizeBytes;
	}

	public String getZookeeperBasePath() {
		return zookeeperBasePath;
	}

	public void setZookeeperBasePath(String zookeeperBasePath) {
		this.zookeeperBasePath = zookeeperBasePath;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getZookeeperQuorumList() {
		return zookeeperQuorumList;
	}

	public void setZookeeperQuorumList(String zookeeperQuorumList) {
		this.zookeeperQuorumList = zookeeperQuorumList;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}	

	public List<String> getSeedBrokerList() {
		return seedBrokerList;
	}

	public void setSeedBrokerList(List<String> seedBrokerList) {
		this.seedBrokerList = seedBrokerList;
	}

	public int getBrokerPort() {
		return brokerPort;
	}

	public void setBrokerPort(int brokerPort) {
		this.brokerPort = brokerPort;
	}
}
