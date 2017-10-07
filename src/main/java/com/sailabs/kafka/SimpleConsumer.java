package com.sailabs.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleConsumer {

	Properties props ;
	String node = "rm01.itversity.com:6667,nn02.itversity.com:6667,nn01.itversity.com:6667";
	
	public void init(){
		
		 props = new Properties();
	     props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, node);
	     props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
	     props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
	     props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
	     props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
	     props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	     
		
		
	}
	
	@SuppressWarnings("resource")
	public void receiveMessage(){
		System.out.println(" consumer receiveMessage ");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		System.out.println("Preparing consumer ");
		consumer.subscribe(Arrays.asList(KafkaClient.TOPIC));
		System.out.println("subscribed topic " + KafkaClient.TOPIC);
	     while (true) {
	    	 System.out.println(" infinite while loop waiting for records ");
	    	 
	         ConsumerRecords<String, String> records = consumer.poll(1000);
	         System.out.println("Received records count " + records.count());
	         
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	     }
		
	}
	
	
}
