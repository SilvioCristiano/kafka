package shortvideo.stream;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;



public class StreamProducer {

	public void producer() {
		
		String authToken = "dd)nIX];zpI9aIWv<2pP";
		String tenancyName = "myemail05";
		String username = "myemail05@gmail.com";
		String streamid = "ocid1.streampool.oc1.iad.amaaaaaa4moiyxaaroikmxakqadcohfsebp3cqoduu62br6pdn6uvapfhebq";
		String topicName = "Stream-java";
		
		Properties properties = new Properties();
		
		properties.put("bootstrap.servers", "cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092");
		properties.put("security.protocol", "SASL_SSL");
		properties.put("sasl.mechanism", "PLAIN");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
				+ tenancyName + "/"
				+ username + "/"
				+ streamid + "\" "
			    + "password=\""
			    + authToken + "\";"
			);
		
		properties.put("", 5);
		properties.put("", 1024 * 1024);
		
		KafkaProducer producer = new KafkaProducer<>(properties);
		
	    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, UUID.randomUUID().toString(), "Test Stream short video ##");
	    producer.send(record, (md, ex) -> {
	    	if(ex != null) {
	    		ex.printStackTrace();
	    	}else {
	    		System.out.println("Msg envianda para " 
	    				+ md.partition()
	    				+ "Com offset "
	    				+ md.offset()
	    				+ " no "
	    				+ md.timestamp()
	    				);
	    	}
	    });
	    
	    producer.flush();
	    producer.close();
	    System.out.println("produced 1 megs");
		
		
	}
}
