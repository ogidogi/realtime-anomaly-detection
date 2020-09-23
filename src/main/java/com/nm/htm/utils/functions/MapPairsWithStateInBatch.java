package com.nm.htm.utils.functions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.htm.ResultState;
import com.nm.htm.kafka.MonitoringRecordState;
import com.nm.htm.utils.Utils;

public class MapPairsWithStateInBatch implements
        Function3<String, Optional<Iterable<ConsumerRecord<String, MonitoringRecord>>>, State<MonitoringRecordState>, Iterable<ConsumerRecord<String, MonitoringRecord>>> {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 669411800475911654L;

    private static final Logger LOGGER = LoggerFactory.getLogger(MapPairsWithStateInBatch.class);
    
    private boolean enableCheckOnDateOrder;
    
    public MapPairsWithStateInBatch(boolean enableCheckOnDateOrder) {
        this.enableCheckOnDateOrder = enableCheckOnDateOrder;
    }
    
    @Override
    public Iterable<ConsumerRecord<String, MonitoringRecord>> call(String key,
            Optional<Iterable<ConsumerRecord<String, MonitoringRecord>>> records, State<MonitoringRecordState> state)
            throws Exception {
        Iterable<ConsumerRecord<String, MonitoringRecord>> result = null;
        if (records.isPresent()) {
            MonitoringRecordState currentState = null;
            if (!state.exists()) {
                currentState = new MonitoringRecordState(key);
            } else {
                currentState = state.get();
            }
            List<ConsumerRecord<String, MonitoringRecord>> validRecords = new ArrayList<>();
            DateTime last = currentState.getLastProcessed();
            for (ConsumerRecord<String, MonitoringRecord> record : records.get()) {
                DateTime recordDate = Utils.parseRecordDate(record.value());
                if (!enableCheckOnDateOrder || last == null || last.compareTo(recordDate) < 0) {
                    validRecords.add(record);
                    last = recordDate;
                }
            }
            List<ResultState> resultStates = currentState.updateState(validRecords);
            Iterator<ResultState> resultStateIterator = resultStates.iterator();
            Iterator<ConsumerRecord<String, MonitoringRecord>> validRecordsIterator = validRecords.iterator();

            while (resultStateIterator.hasNext() && validRecordsIterator.hasNext()) {
                ResultState rs = resultStateIterator.next();
                ConsumerRecord<String, MonitoringRecord> record = validRecordsIterator.next();

                record.value().setPrediction(rs.getPrediction());
                record.value().setError(rs.getError());
                record.value().setAnomaly(rs.getAnomaly());
                record.value().setPredictionNext(rs.getPredictionNext());
            }

            state.update(currentState);
            result = validRecords;
        }
        return result;
    }

}
