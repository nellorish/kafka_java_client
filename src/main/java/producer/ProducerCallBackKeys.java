package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerCallBackKeys {


    public static void main(String[] args) throws ExecutionException, InterruptedException {

       final  Logger logger = LoggerFactory.getLogger(ProducerCallBackKeys.class.getName());

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String,String> kafka = new KafkaProducer<String, String>(props);
        for(int i=0;i<20;i++) {

            String topic = "first_topic";
            String message = "Hello World with Key Partition"+ i;
            String key = "id_"+i;

            logger.info("Value of the Key "+key);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,message);


            kafka.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Message Send to " +
                                "\n Topic :" + recordMetadata.topic() +
                                "\n Partition :" + recordMetadata.partition() +
                                "\n Offset :" + recordMetadata.offset());
                    } else {
                        logger.error(e.getMessage());
                    }
                }
            }).get();
        }
        //kafka.send(record);

        kafka.flush();
        kafka.close();
        System.out.println("Java is running as expected");
    }
}
