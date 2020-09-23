package com.nm.htm.kafka;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.utils.PropertiesLoader;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;

import java.util.Arrays;
import java.util.Collection;

public class KafkaHelper {

    public static KafkaProducer<String, MonitoringRecord> createJsonProducer() {
        return new KafkaProducer<>(PropertiesLoader.getKafkaJsonProducerProperties());
    }

    public static KafkaProducer<String, MonitoringRecord> createKryoProducer() {
        return new KafkaProducer<>(PropertiesLoader.getKafkaKryoProducerProperties());
    }

    public static ConsumerStrategy<String, MonitoringRecord> createKryoConsumerStrategy(String topics) {
        Collection<String> topicsList = Arrays.asList(topics.split(","));
        return ConsumerStrategies.Subscribe(topicsList, PropertiesLoader.getKafkaKryoConsumerProperties());
    }

    public static ConsumerStrategy<String, MonitoringRecord> createJsonConsumerStrategy(String topics) {
        Collection<String> topicsList = Arrays.asList(topics.split(","));
        return ConsumerStrategies.Subscribe(topicsList, PropertiesLoader.getKafkaJsonConsumerProperties());
    }

    public static String getKey(MonitoringRecord record) {
        return record.getStateCode() + "-" + record.getCountyCode() + "-" + record.getSiteNum() + "-"
                + record.getParameterCode() + "-" + record.getPoc();
    }

    public static KafkaConsumer<String, MonitoringRecord> createJsonConsumer() {
        return new KafkaConsumer<String, MonitoringRecord>(PropertiesLoader.getKafkaJsonConsumerProperties());
    }

}
