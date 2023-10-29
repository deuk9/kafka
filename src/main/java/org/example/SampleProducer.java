package org.example;

import com.sun.jdi.Bootstrap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.Properties;

public class SampleProducer {

    private final static Logger logger = LoggerFactory.getLogger(SampleProducer.class);
    private KafkaProducer<String, String> producer;
    public void init(){
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "1");

        producer = new KafkaProducer<>(config);
        logger.info("initialize kafka producer");
    }

    public void initCustomPartitioner(){
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(config);
        logger.info("initialize kafka producer");
    }

    public void send(){
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "testMessage");
        try {
            producer.send(record);
            System.out.println(record);
//            producer.flush();
//            producer.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void sendHasKey(){
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "first", "testMessage");
        try {
            producer.send(record);
            System.out.println(record);
//            producer.flush();
//            producer.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void sendHasKeyAndPartition(){
        ProducerRecord<String, String> record = new ProducerRecord<>("test", 0,"first", "testMessage");
        try {
            producer.send(record);
            System.out.println(record);
//            producer.flush();
//            producer.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void sendThenConfirm(){
        ProducerRecord<String, String> record = new ProducerRecord<>("test", "first", "testMessage");
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println(recordMetadata.toString());
//            producer.flush();
//            producer.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }



    public void stop(){
        producer.flush();
        producer.close();
    }
}
