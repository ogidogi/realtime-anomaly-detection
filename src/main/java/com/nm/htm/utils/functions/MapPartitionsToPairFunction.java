package com.nm.htm.utils.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.nm.htm.htm.MonitoringRecord;

import scala.Tuple2;

public class MapPartitionsToPairFunction implements
        PairFlatMapFunction<Iterator<ConsumerRecord<String, MonitoringRecord>>, String, Iterable<ConsumerRecord<String, MonitoringRecord>>> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 2420663004779077525L;

    @Override
    public Iterator<Tuple2<String, Iterable<ConsumerRecord<String, MonitoringRecord>>>> call(
            Iterator<ConsumerRecord<String, MonitoringRecord>> iterator) throws Exception {
        Map<String, List<ConsumerRecord<String, MonitoringRecord>>> map = new HashMap<>();
        while (iterator.hasNext()) {
            ConsumerRecord<String, MonitoringRecord> record = iterator.next();
            List<ConsumerRecord<String, MonitoringRecord>> list = map.get(record.key());
            if (list == null) {
                list = new ArrayList<>();
                map.put(record.key(), list);
            }
            list.add(record);
        }
        List<Tuple2<String, Iterable<ConsumerRecord<String, MonitoringRecord>>>> tuples = new ArrayList<>();
        for (Map.Entry<String, List<ConsumerRecord<String, MonitoringRecord>>> entry : map.entrySet()) {
            tuples.add(new Tuple2<String, Iterable<ConsumerRecord<String, MonitoringRecord>>>(entry.getKey(), entry.getValue()));
        }
        return tuples.iterator();
    }

}
