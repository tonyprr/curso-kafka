package com.tonyprr.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class DevsjProducer {

    public static  final Logger log = LoggerFactory.getLogger(DevsjProducer.class);
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props=new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        //props.setProperty("group.id","devs4j-group");
        //props.setProperty("enable.auto.commit","true");
        //props.setProperty("auto.commit.interval.ms","1000");
        props.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("linger.ms", "12");

        try(Producer<String, String>producer=new
                KafkaProducer<>(props);) {
            for(int i= 0; i< 100000 ;i++) {
//                try {
                    producer.send(new
                            ProducerRecord<>("devs4j-topic",String.valueOf(i),"message2"));//.get();
//                } catch (InterruptedException| ExecutionException e) {
//                    log.error("Message producer interrupted ", e);
//                }
            }
            producer.flush();
        }
        // 3840 ms  to send 100000
        // 3840 ms  to send 100000  -- linger 0
        // 3766 ms  to send 100000  -- linger 6
        // 3002 ms  to send 100000  -- linger 10
        // 2384 ms  to send 100000  -- linger 12
        // 3133 ms  to send 100000  -- linger 15
        log.info("Processing time = {} ms ", (System.currentTimeMillis() - startTime));
    }
}
