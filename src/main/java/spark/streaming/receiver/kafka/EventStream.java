package spark.streaming.receiver.kafka;

import java.io.Serializable;

public class EventStream implements Serializable {

	private byte[] payload;
	private byte[] key;
	private long offset;
	private String topic;
	private int partition;
	private String clientId;	
	private String brokerHost;
	private int brokerPort;
	private String zookeeperQuorumList;	
	private String zookeeperBasePath;
	

	public String getZookeeperBasePath() {
		return zookeeperBasePath;
	}

	public void setZookeeperBasePath(String zookeeperBasePath) {
		this.zookeeperBasePath = zookeeperBasePath;
	}

	public String getZookeeperQuorumList() {
		return zookeeperQuorumList;
	}

	public void setZookeeperQuorumList(String zookeeperQuorumList) {
		this.zookeeperQuorumList = zookeeperQuorumList;
	}

	public String getBrokerHost() {
		return brokerHost;
	}

	public void setBrokerHost(String brokerHost) {
		this.brokerHost = brokerHost;
	}

	public int getBrokerPort() {
		return brokerPort;
	}

	public void setBrokerPort(int brokerPort) {
		this.brokerPort = brokerPort;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public byte[] getKey() {
		return key;
	}

	public void setKey(byte[] key) {
		this.key = key;
	}	

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
}
