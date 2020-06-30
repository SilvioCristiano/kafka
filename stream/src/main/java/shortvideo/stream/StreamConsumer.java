package shortvideo.stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StreamConsumer {
	
	public void consume() {
		
		String authToken = "dd)nIX];zpI9aIWv<2pP";
		String tenancyName = "myemail05";
		String username = "myemail05@gmail.com";
		String streamid = "ocid1.streampool.oc1.iad.amaaaaaa4moiyxaaroikmxakqadcohfsebp3cqoduu62br6pdn6uvapfhebq";
		String topicName = "Stream-java";
		
		Properties properties = new Properties();
        properties.put("bootstrap.servers", "cell-1.streaming.us-ashburn-1.oci.oraclecloud.com:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-0");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + tenancyName + "/"
                        + username + "/"
                        + streamid + "\" "
                        + "password=\""
                        + authToken + "\";"
        );
        properties.put("max.partition.fetch.bytes", 1024 * 1024); 

        Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
        try {
            consumer.subscribe(Collections.singletonList( topicName ) );

            while(true) {
                Duration duration = Duration.ofMillis(1000);
                ConsumerRecords<Long, String> consumerRecords = consumer.poll(duration);
                consumerRecords.forEach(record -> {
                    System.out.println("Record Key " + record.key());
                    System.out.println("Record value " + record.value());
                    System.out.println("Record partition " + record.partition());
                    System.out.println("Record offset " + record.offset());
                });
             
                consumer.commitAsync();
            }
        }
        catch(WakeupException e) {
          
        }
        finally {
            System.out.println("closing consumer");
            consumer.close();
        }

		
		
	}

}
