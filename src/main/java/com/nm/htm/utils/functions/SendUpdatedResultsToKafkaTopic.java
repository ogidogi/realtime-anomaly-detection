package com.nm.htm.utils.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.kafka.KafkaHelper;

public class SendUpdatedResultsToKafkaTopic implements Function<Iterator<Iterable<ConsumerRecord<String, MonitoringRecord>>>, List<OffsetRange>> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 508260655592025514L;

    private static final Logger LOGGER = LoggerFactory.getLogger(SendUpdatedResultsToKafkaTopic.class);

    private String topic;

    public SendUpdatedResultsToKafkaTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public List<OffsetRange> call(Iterator<Iterable<ConsumerRecord<String, MonitoringRecord>>> iterator) throws Exception {
        List<OffsetRange> offsets = new ArrayList<>();
        try (KafkaProducer<String, MonitoringRecord> producer = KafkaHelper.createJsonProducer()) {
            while (iterator.hasNext()) {
                Iterable<ConsumerRecord<String, MonitoringRecord>> list = iterator.next();
                long rangeEnds = Long.MIN_VALUE, rangeStarts = Long.MAX_VALUE;
                String recordsTopic = null;
                Integer partition = null;
                for (ConsumerRecord<String, MonitoringRecord> record : list) {
                    rangeEnds = Math.max(rangeEnds, record.offset());
                    rangeStarts = Math.min(rangeStarts, record.offset());
                    if(recordsTopic == null) {
                        recordsTopic = record.topic();
                    }
                    if(partition == null) {
                        partition = record.partition();
                    }
                    
                    ProducerRecord<String, MonitoringRecord> kafkaRecord = new ProducerRecord<>(topic,
                            KafkaHelper.getKey(record.value()), record.value());
                    LOGGER.debug("MonitoringRecord: {}", kafkaRecord);
                    producer.send(kafkaRecord);
                }
                if(recordsTopic != null && partition != null) {
                    offsets.add(OffsetRange.create(recordsTopic, partition, rangeStarts, rangeEnds));
                }
            }
            producer.flush();
        }
        
        return offsets;
    }

}
