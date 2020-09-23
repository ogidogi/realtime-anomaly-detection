package com.nm.htm.kafka;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.utils.GlobalConstants;
import com.nm.htm.utils.PropertiesLoader;
import com.opencsv.CSVReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.nm.htm.kafka.KafkaHelper.getKey;

public class TopicGenerator implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicGenerator.class);

    public static void main(String[] args) {
        // load a properties file from class path, inside static method
        Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final boolean skipHeader = Boolean
                    .parseBoolean(applicationProperties.getProperty(GENERATOR_SKIP_HEADER_CONFIG));
            final long batchSleep = Long.parseLong(applicationProperties.getProperty(GENERATOR_BATCH_SLEEP_CONFIG));
            final int batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_CONFIG));
            final String sampleFile = applicationProperties.getProperty(GENERATOR_SAMPLE_FILE_CONFIG);
            final String topicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            long time = System.nanoTime();

            // read the file and push the record to Kafka
            try (CSVReader reader = new CSVReader(new FileReader(sampleFile));
                 KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createKryoProducer()) {
                int i = 0;
                String[] line;
                while ((line = reader.readNext()) != null) {
                    i++;
                    if ((i == 1) && skipHeader) {
                        continue;
                    }

                    MonitoringRecord record = new MonitoringRecord(line);
                    String key = getKey(record);
                    ProducerRecord<String, MonitoringRecord> kafkaRecord = new ProducerRecord<>(topicName, key, record);
                    Future<RecordMetadata> response = producer.send(kafkaRecord);

                    if (i % batchSize == 0) {
                        LOGGER.info("Offset: {}", response.get().offset());
                    }
                }
                LOGGER.debug("TopicGenerator.main send all records time: {}", String.valueOf(System.nanoTime() - time));
            } catch (FileNotFoundException e) {
                LOGGER.error("Sorry, unable to find sample file", e);
            } catch (IOException e) {
                LOGGER.error("Sorry, unable to read sample file", e);
            } catch (InterruptedException e) {
                LOGGER.error("Sorry, thread was interrupted", e);
            } catch (ExecutionException e) {
                LOGGER.error("Sorry, the computation threw an exception", e);
            }
        }
    }
}
