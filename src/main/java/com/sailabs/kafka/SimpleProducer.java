package com.sailabs.kafka;

import java.util.Calendar;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleProducer {

	String node = "rm01.itversity.com:6667,nn02.itversity.com:6667,nn01.itversity.com:6667";
	//String node = "0.0.0.0:6667";
	String keySer = "org.apache.kafka.common.serialization.StringSerializer";
	String valueSer = "org.apache.kafka.common.serialization.StringSerializer";

	Properties pros;
	RecordCallBack mCallBack;

	public void init() {
		pros = new Properties();
		pros.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, node);
		pros.put(ProducerConfig.RETRIES_CONFIG, 1);
		pros.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
		pros.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		pros.put("security.protocol", "PLAINTEXT");
		pros.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer);
		pros.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer);

		mCallBack = new RecordCallBack();

	}

	public void createMessage() {

		try {
			// Thread.currentThread().setContextClassLoader(null);
			Producer<String, String> producer = new KafkaProducer<>(pros);

			for (int i = 0; i < 50; i++) {
				/*try {
					Thread.sleep(1000);
				} catch (Exception e) {

				}*/
				System.out.println(Calendar.getInstance().getTime()+ " \t preparing data to producer" );
				ProducerRecord<String, String> pr = new ProducerRecord<String, String>(KafkaClient.TOPIC, "Key " + i,
						" Value " + i);
				System.out.println("Record prepared , yet to send ");
				producer.send(pr,new RecordCallBack());
				System.out.println(Calendar.getInstance().getTime()+" \t sending data from producer");
			}
		} catch (Exception e) {
			e.printStackTrace();
			// System.err.print(e.printStackTrace());
		}
	}

	private static class RecordCallBack implements Callback {

		@Override
		public void onCompletion(RecordMetadata metaData, Exception exception) {

			if (null != exception) {
				System.out.println("Error in sending record Exception " + exception );

			} else {
				System.out.println(" Message sent successfully with topic " + metaData.topic() );
			}

		}

	}

}
