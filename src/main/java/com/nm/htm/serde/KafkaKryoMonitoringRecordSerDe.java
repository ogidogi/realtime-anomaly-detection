
package com.nm.htm.serde;

import java.util.Map;

import com.nm.htm.htm.MonitoringRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.esotericsoftware.kryo.serializers.BeanSerializer;

public class KafkaKryoMonitoringRecordSerDe implements Serializer<MonitoringRecord>, Deserializer<MonitoringRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaKryoMonitoringRecordSerDe.class);
    private ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        kryo.addDefaultSerializer(MonitoringRecord.class, new BeanSerializer<MonitoringRecord>(kryo, MonitoringRecord.class));
        return kryo;
    });

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String s, MonitoringRecord record) {
        long time = System.nanoTime();
        Output output = new UnsafeOutput(SizeCache.getSize(record));
        kryos.get().writeObject(output, record);
        LOGGER.debug("KafkaKryoMonitoringRecordSerDe.serialize time: {}", String.valueOf(System.nanoTime() - time));
        return output.toBytes();
    }

    @Override
    public MonitoringRecord deserialize(String s, byte[] bytes) {
        long time = System.nanoTime();
        try {
        	Input input = new UnsafeInput(bytes);
            MonitoringRecord record = kryos.get().readObject(input, MonitoringRecord.class);
            LOGGER.debug("KafkaKryoMonitoringRecordSerDe.deserialize time: {}", String.valueOf(System.nanoTime() - time));
            return record;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }
    
    private static class SizeCache {
    	private static final long SIZE_MULTIPLIER = 3;
    	private static Long size;
    	
    	public static int getSize(MonitoringRecord record) {
    		if(size == null) {
    			synchronized (SizeCache.class) {
					if(size == null) {
				        int bytesCount = 1;
				        bytesCount += record.getStateCode().getBytes().length + 2;
				        bytesCount += record.getCountyCode().getBytes().length + 2;
				        bytesCount += record.getSiteNum().getBytes().length + 2;
				        bytesCount += record.getParameterCode().getBytes().length + 2;
				        bytesCount += record.getPoc().getBytes().length + 2;
				        bytesCount += record.getLatitude().getBytes().length + 2;
				        bytesCount += record.getLongitude().getBytes().length + 2;
				        bytesCount += record.getDatum().getBytes().length + 2;
				        bytesCount += record.getParameterName().getBytes().length + 2;
				        bytesCount += record.getDateLocal().getBytes().length + 2;
				        bytesCount += record.getTimeLocal().getBytes().length + 2;
				        bytesCount += record.getDateGMT().getBytes().length + 2;
				        bytesCount += record.getTimeGMT().getBytes().length + 2;
				        bytesCount += record.getSampleMeasurement().getBytes().length + 2;
				        bytesCount += record.getUnitsOfMeasure().getBytes().length + 2;
				        bytesCount += record.getMdl().getBytes().length + 2;
				        bytesCount += record.getUncertainty().getBytes().length + 2;
				        bytesCount += record.getQualifier().getBytes().length + 2;
				        bytesCount += record.getMethodType().getBytes().length + 2;
				        bytesCount += record.getMethodCode().getBytes().length + 2;
				        bytesCount += record.getMethodName().getBytes().length + 2;
				        bytesCount += record.getStateName().getBytes().length + 2;
				        bytesCount += record.getCountyName().getBytes().length + 2;
				        bytesCount += record.getDateOfLastChange().getBytes().length + 2;

				        bytesCount += 8 * 4;
			    		size = new Long(bytesCount * SIZE_MULTIPLIER);
					}
				}
    		}
    		return size.intValue();
    	}
    }
    
    @Override
    public void close() {
    }

}
