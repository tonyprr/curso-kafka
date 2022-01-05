package com.tonyprr.kafka.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class DevsjThreadConsumer extends Thread {

    private final KafkaConsumer<String, String> consumer;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public static  final Logger log = LoggerFactory.getLogger(DevsjThreadConsumer.class);

    public DevsjThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("devs4j-topic"));
        try {
            while (!closed.get()) {
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                    for(ConsumerRecord<String, String> record :records) {
                        log.debug("offset = {}, partition = {}, key = {}, value ={}",
                                record.offset(), record.partition(), record.key(), record.value());
                        if ((Integer.parseInt(record.key()) % 10000) == 0) {
                            log.info("offset = {}, partition = {}, key = {}, value ={}",
                                    record.offset(), record.partition(), record.key(), record.value());
                        }
                    }
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
