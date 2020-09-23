package com.nm.htm.serde;

import com.nm.htm.htm.MonitoringRecord;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class KafkaJsonMonitoringRecordSerDe implements Deserializer<MonitoringRecord>, Serializer<MonitoringRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJsonMonitoringRecordSerDe.class);

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, MonitoringRecord data) {
        long time = System.nanoTime();
        byte[] retVal = null;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            retVal = objectMapper.writeValueAsBytes(data);

            LOGGER.debug("MonitoringRecord {} serialized into {} bytes", data, retVal.length);
        } catch (JsonProcessingException e) {
            LOGGER.error("Sorry, JsonProcessingException for MonitoringRecord in KafkaJsonMonitoringRecordSerDe", e);
        }
        LOGGER.debug("KafkaJsonMonitoringRecordSerDe.serialize time: {}", String.valueOf(System.nanoTime() - time));
        return retVal;
    }

    @Override
    public MonitoringRecord deserialize(String topic, byte[] data) {
        long time = System.nanoTime();
        ObjectMapper mapper = new ObjectMapper();

        MonitoringRecord record = null;
        try {
            record = mapper.readValue(data, MonitoringRecord.class);
            LOGGER.debug("{} bytes decoded into object {}", data.length, record);
        } catch (JsonParseException e) {
            LOGGER.error("Sorry, JsonParseException for MonitoringRecord in KafkaJsonMonitoringRecordSerDe", e);
        } catch (JsonMappingException e) {
            LOGGER.error("Sorry, JsonMappingException for MonitoringRecord in KafkaJsonMonitoringRecordSerDe", e);
        } catch (IOException e) {
            LOGGER.error("Sorry, IOException for MonitoringRecord in KafkaJsonMonitoringRecordSerDe", e);
        }
        LOGGER.debug("KafkaJsonMonitoringRecordSerDe.deserialize time: {}", String.valueOf(System.nanoTime() - time));
        return record;
    }

    @Override
    public void close() {
    }
}