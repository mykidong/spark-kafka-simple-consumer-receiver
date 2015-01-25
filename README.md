# spark-kafka-simple-consumer-receiver
Thanks to dibbhatt(https://github.com/dibbhatt/kafka-spark-consumer) .
With the help of his ideas which I got from him when we discussed in emails, it could be done.
This Spark Streaming Kafka Receiver is almost based on https://github.com/dibbhatt/kafka-spark-consumer .

This Spark Streaming Kafka Receiver is alternative to the current Spark Streaming Kafka Receiver which is written in Kafka High Level Consumer API.
Because this kafka receiver is written in Kafka Simple Consumer API, kafka message offset can be controlled with ease.


# Run Kafka Receiver Test Case
In KafkaSimpleReceiverTest.java, the following line should be changed to suit your needs:
    		
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


DStream generated from Kafka Receiver looks like this:


		JavaDStream<EventStream> unionStreams = KafkaReceiverUtils.createStream(ssc, 
											topicTokens, 
											partitionCount, 
											zookeeperBasePath, 
											zookeeperQuorumList, 
											brokerList, 
											brokerPort, 
											clientId, 
											fetchSizeBytes);




Run maven command like this:

      mvn -e -Dtest=KafkaSimpleReceiverTest test;


# Kafka ETL Demo Test Case
Originally, with this kafka receiver, I intended to write the Kafka ETL instead of using camus.
You can find some codes of Kafka ETL Demo Test Case in KafkaSimpleEtlTestSkip.java, 
in which you can get some idea how to use JavaDStream&lt;EventStream&gt; generated from this kafka receiver.
  

