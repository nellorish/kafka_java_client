package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;


import java.util.Properties;

public class ProducerCallBackDemo {


    public static void main(String[] args) {

       final  Logger logger = LoggerFactory.getLogger(ProducerCallBackDemo.class.getName());

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> kafka = new KafkaProducer<String, String>(props);
        for(int i=0;i<10;i++){
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic", "Hello World from Other Side With Partitons");
        kafka.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null) {
                  logger.info("Message Send to " +
                               "\n Topic :"+ recordMetadata.topic()+
                               "\n Partition :"+ recordMetadata.partition()+
                               "\n Offset :"+ recordMetadata.offset());


                } else {
                    logger.error(e.getMessage());
                }
            }
        });
   }
        //kafka.send(record);

        kafka.flush();
        kafka.close();
        System.out.println("Java is running as expected");
    }
}
