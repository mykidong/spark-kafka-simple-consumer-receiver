package mykidong.spark.receiver;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class KafkaSimpleReceiver extends Receiver<EventStream> {
	
	private Thread t;	
	
	private KafkaConfiguration kafkaConfiguration;
	KafkaSimpleConsumer kafkaSimpleConsumer;

	public KafkaSimpleReceiver(KafkaConfiguration kafkaConfiguration) {
		super(StorageLevel.MEMORY_ONLY_SER());

		this.kafkaConfiguration = kafkaConfiguration;
	}

	@Override
	public void onStart() {
		
		kafkaSimpleConsumer = new KafkaSimpleConsumer(this, this.kafkaConfiguration);		
	
		t = new Thread(kafkaSimpleConsumer);	
		t.start();		
	}

	@Override
	public void onStop() {
		this.kafkaSimpleConsumer.stop();
		
		t = null;
	}

}
