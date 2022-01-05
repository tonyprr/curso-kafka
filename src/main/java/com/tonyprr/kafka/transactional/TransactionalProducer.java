package com.tonyprr.kafka.transactional;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TransactionalProducer {

    public static  final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties props=new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        props.setProperty("acks","all");
        props.setProperty("transactional.id","devs4j-producer-id");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("linger.ms", "12");

        try(Producer<String, String> producer = new KafkaProducer<>(props);) {
            producer.initTransactions();
            producer.beginTransaction();
            for(int i= 0; i< 100000 ;i++) {
                producer.send(new
                        ProducerRecord<>("devs4j-topic",String.valueOf(i),"message2"));//.get();
            }
            producer.flush();
        }
        log.info("Processing time = {} ms ", (System.currentTimeMillis() - startTime));

    }
}
