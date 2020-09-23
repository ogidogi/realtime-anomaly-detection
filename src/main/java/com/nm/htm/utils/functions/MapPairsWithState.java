package com.nm.htm.utils.functions;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.htm.ResultState;
import com.nm.htm.kafka.MonitoringRecordState;
import com.nm.htm.utils.Utils;

public class MapPairsWithState implements
        Function3<String, Optional<Iterable<ConsumerRecord<String, MonitoringRecord>>>, State<MonitoringRecordState>, Iterable<ConsumerRecord<String, MonitoringRecord>>> {

    /**
     * 5426938473977257053L
     */
    private static final long serialVersionUID = 5426938473977257053L;

    private boolean enableCheckOnDateOrder;
    
    public MapPairsWithState(boolean enableCheckOnDateOrder) {
        this.enableCheckOnDateOrder = enableCheckOnDateOrder;
    }
    
    @Override
    public Iterable<ConsumerRecord<String, MonitoringRecord>> call(String key, Optional<Iterable<ConsumerRecord<String, MonitoringRecord>>> records,
            State<MonitoringRecordState> state) throws Exception {
        Iterable<ConsumerRecord<String, MonitoringRecord>> result = null;
        if (records.isPresent()) {
            MonitoringRecordState currentState = null;
            if (!state.exists()) {
                currentState = new MonitoringRecordState(key);
            } else {
                currentState = state.get();
            }
            for(ConsumerRecord<String, MonitoringRecord> record: records.get()) {
                if(!enableCheckOnDateOrder || Utils.isValidDateForNetwork(record.value(), currentState)) {
                    ResultState rs = currentState.updateState(record.value());
                    
                    record.value().setPrediction(rs.getPrediction());
                    record.value().setError(rs.getError());
                    record.value().setAnomaly(rs.getAnomaly());
                    record.value().setPredictionNext(rs.getPredictionNext());
                }
            }
            state.update(currentState);
            result = records.get();
        }
        return result;
    }

}
