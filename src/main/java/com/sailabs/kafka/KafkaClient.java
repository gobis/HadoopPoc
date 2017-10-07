package com.sailabs.kafka;

public class KafkaClient {

	public static String TOPIC = "gobi_test_topic";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub


		SimpleConsumer consumer = new SimpleConsumer();
		consumer.init();
		Thread t2 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				
				consumer.receiveMessage();
			}
		});
		
		t2.start();
		
		
		
		
		SimpleProducer producer = new SimpleProducer();
		producer.init();
		
		Thread t1 = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				producer.createMessage();
			}
		});
		t1.start();
		
		
		
	}

}
