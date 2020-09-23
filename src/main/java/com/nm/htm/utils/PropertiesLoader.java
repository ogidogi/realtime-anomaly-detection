package com.nm.htm.utils;

import com.nm.htm.kafka.MonitoringRecordPartitioner;
import com.nm.htm.serde.KafkaJsonMonitoringRecordSerDe;
import com.nm.htm.serde.KafkaKryoMonitoringRecordSerDe;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.nm.htm.utils.GlobalConstants.PROPERTIES_FILE;
import static com.nm.htm.utils.GlobalConstants.PROPERTIES_FILE_NAME;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class PropertiesLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertiesLoader.class);

    private static final Properties globalProperties = new Properties();
    private static final Properties kafkaJsonProducerProperties = new Properties();
    private static final Properties kafkaKryoProducerProperties = new Properties();
    private static final Map<String, Object> kafkaJsonConsumerProperties = new HashMap<>();
    private static final Map<String, Object> kafkaKryoConsumerProperties = new HashMap<>();

    static {

        try (InputStream input = (PropertiesLoader.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE) == null ? new FileInputStream(System.getProperty("user.dir") + "/" + PROPERTIES_FILE_NAME) : PropertiesLoader.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE))) {
            globalProperties.load(input);
            LOGGER.info(String.valueOf(globalProperties));

            if (!globalProperties.isEmpty()) {
                kafkaJsonProducerProperties.put(BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
                kafkaJsonProducerProperties.put(ACKS_CONFIG, globalProperties.getProperty(ACKS_CONFIG));
                kafkaJsonProducerProperties.put(RETRIES_CONFIG, globalProperties.getProperty(RETRIES_CONFIG));
                kafkaJsonProducerProperties.put(BATCH_SIZE_CONFIG, globalProperties.getProperty(BATCH_SIZE_CONFIG));
                kafkaJsonProducerProperties.put(LINGER_MS_CONFIG, globalProperties.getProperty(LINGER_MS_CONFIG));
                kafkaJsonProducerProperties.put(BUFFER_MEMORY_CONFIG, globalProperties.getProperty(BUFFER_MEMORY_CONFIG));
                kafkaJsonProducerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                kafkaJsonProducerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class);
                kafkaJsonProducerProperties.put(PARTITIONER_CLASS_CONFIG, MonitoringRecordPartitioner.class);

                kafkaKryoProducerProperties.put(BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
                kafkaKryoProducerProperties.put(ACKS_CONFIG, globalProperties.getProperty(ACKS_CONFIG));
                kafkaKryoProducerProperties.put(RETRIES_CONFIG, globalProperties.getProperty(RETRIES_CONFIG));
                kafkaKryoProducerProperties.put(BATCH_SIZE_CONFIG, globalProperties.getProperty(BATCH_SIZE_CONFIG));
                kafkaKryoProducerProperties.put(LINGER_MS_CONFIG, globalProperties.getProperty(LINGER_MS_CONFIG));
                kafkaKryoProducerProperties.put(BUFFER_MEMORY_CONFIG, globalProperties.getProperty(BUFFER_MEMORY_CONFIG));
                kafkaKryoProducerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                kafkaKryoProducerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, KafkaKryoMonitoringRecordSerDe.class);
                kafkaKryoProducerProperties.put(PARTITIONER_CLASS_CONFIG, MonitoringRecordPartitioner.class);

                kafkaJsonConsumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
                kafkaJsonConsumerProperties.put(GROUP_ID_CONFIG, globalProperties.getProperty(GROUP_ID_CONFIG));
                kafkaJsonConsumerProperties.put(AUTO_OFFSET_RESET_CONFIG, globalProperties.getProperty(AUTO_OFFSET_RESET_CONFIG));
                kafkaJsonConsumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, globalProperties.getProperty(ENABLE_AUTO_COMMIT_CONFIG));
                kafkaJsonConsumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                kafkaJsonConsumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonMonitoringRecordSerDe.class);

                kafkaKryoConsumerProperties.put(BOOTSTRAP_SERVERS_CONFIG, globalProperties.getProperty(BOOTSTRAP_SERVERS_CONFIG));
                kafkaKryoConsumerProperties.put(GROUP_ID_CONFIG, globalProperties.getProperty(GROUP_ID_CONFIG));
                kafkaKryoConsumerProperties.put(AUTO_OFFSET_RESET_CONFIG, globalProperties.getProperty(AUTO_OFFSET_RESET_CONFIG));
                kafkaKryoConsumerProperties.put(ENABLE_AUTO_COMMIT_CONFIG, globalProperties.getProperty(ENABLE_AUTO_COMMIT_CONFIG));
                kafkaKryoConsumerProperties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                kafkaKryoConsumerProperties.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaKryoMonitoringRecordSerDe.class);
            }
        } catch (IOException e) {
            LOGGER.error("Sorry, unable to read properties file", e);
        }
    }

    public static Properties getGlobalProperties() {
        return globalProperties;
    }

    public static Properties getKafkaJsonProducerProperties() {
        return kafkaJsonProducerProperties;
    }

    public static Properties getKafkaKryoProducerProperties() {
        return kafkaKryoProducerProperties;
    }

    public static Map<String, Object> getKafkaJsonConsumerProperties() {
        return kafkaJsonConsumerProperties;
    }

    public static Map<String, Object> getKafkaKryoConsumerProperties() {
        return kafkaKryoConsumerProperties;
    }

}
