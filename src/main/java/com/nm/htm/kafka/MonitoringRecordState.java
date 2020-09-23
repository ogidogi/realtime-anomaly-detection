package com.nm.htm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.htm.ResultState;
import com.nm.htm.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.DateTime;

import com.nm.htm.htm.HTMNetwork;

public class MonitoringRecordState implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 2890457621580045780L;

    private HTMNetwork htmNetwork;
    private DateTime lastProcessed;

    public MonitoringRecordState() {
    }
    
    public MonitoringRecordState(String diviceId) throws Exception {
        htmNetwork = new HTMNetwork();
        htmNetwork.setId(diviceId);
        htmNetwork.createNetwork();
    }
    
    public ResultState updateState(MonitoringRecord record) {
        Map<String, Object> m = new HashMap<>();
        DateTime recordDate = Utils.parseRecordDate(record);
        m.put("DT", recordDate);
        m.put("Measurement", Double.parseDouble(record.getSampleMeasurement()));
        ResultState resultState = htmNetwork.compute(m);
        lastProcessed = recordDate;
        return resultState;
    }
    
    public List<ResultState> updateState(Iterable<ConsumerRecord<String, MonitoringRecord>> records) {
        List<Map<String, Object>> inputs = new ArrayList<>();
        for(ConsumerRecord<String, MonitoringRecord> record: records) {
            Map<String, Object> map = new HashMap<>();
            DateTime recordDate = Utils.parseRecordDate(record.value());
            map.put("DT", recordDate);
            map.put("Measurement", Double.parseDouble(record.value().getSampleMeasurement()));
            inputs.add(map);
            lastProcessed = recordDate;
        }
        return htmNetwork.compute(inputs);
    }
    
    public HTMNetwork getHtmNetwork() {
        return htmNetwork;
    }

    public void setHtmNetwork(HTMNetwork htmNetwork) {
        this.htmNetwork = htmNetwork;
    }

    public DateTime getLastProcessed() {
        return lastProcessed;
    }

    public void setLastProcessed(DateTime lastProcessed) {
        this.lastProcessed = lastProcessed;
    }

}
