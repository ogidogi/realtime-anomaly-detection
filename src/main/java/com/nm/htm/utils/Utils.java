package com.nm.htm.utils;

import java.util.Comparator;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.joda.time.DateTime;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.kafka.KafkaHelper;
import com.nm.htm.kafka.MonitoringRecordState;

import scala.Tuple2;

public class Utils implements GlobalConstants {
    private static final Logger LOGGER = Logger.getLogger(Utils.class);

    public static Comparator<MonitoringRecord> monitoringRecordsDateComparator = new Comparator<MonitoringRecord>() {
        @Override
        public int compare(MonitoringRecord o1, MonitoringRecord o2) {
            int result = 0;
            DateTime o1Time = parseRecordDate(o1);
            DateTime o2Time = parseRecordDate(o2);
            if (o1Time != null && o2Time != null) {
                result = o1Time.compareTo(o2Time);
            }
            return result;
        }
    };
    
    public static PairFlatMapFunction<Iterator<ConsumerRecord<String, MonitoringRecord>>, String, Iterable<MonitoringRecord>> mapPartitionsToPairFunc = 
            new PairFlatMapFunction<Iterator<ConsumerRecord<String, MonitoringRecord>>, String, Iterable<MonitoringRecord>>() {
        @Override
        public Iterator<Tuple2<String, Iterable<MonitoringRecord>>> call(
                Iterator<ConsumerRecord<String, MonitoringRecord>> t) throws Exception {

            return null;
        }
    };

    public static DateTime parseRecordDate(MonitoringRecord record) {
        DateTime result = null;
        try {
            result = DateTime.parse(record.getDateGMT() + " " + record.getTimeGMT(), DATE_FORMAT);
        } catch (IllegalArgumentException e) {
            LOGGER.error(e);
            result = null;
        }
        return result;
    }
    
    public static boolean isValidDateForNetwork(MonitoringRecord record, MonitoringRecordState recordState) {
        boolean result = false;
        if (recordState != null && record != null) {
            DateTime recordDate = parseRecordDate(record);
            if (recordState.getLastProcessed() == null
                    || recordDate != null && recordState.getLastProcessed().compareTo(recordDate) < 0) {
                result = true;
            }
        }
        return result;
    }
    
    public static Integer getPartitionsCountforTopic(String topicName) {
        Integer result = null;
        try(KafkaConsumer<String, MonitoringRecord> consumer = KafkaHelper.createJsonConsumer()) {
            result = consumer.partitionsFor(topicName).size();
        }
        return result;
    }

}
