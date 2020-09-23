package com.nm.htm.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaKryoStringSerDe implements Serializer<String>, Deserializer<String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaKryoStringSerDe.class);

    private ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(() -> {
        return new Kryo();
    });

    @Override
    public String deserialize(String topic, byte[] data) {
        long time = System.nanoTime();
        try {
            String value = kryos.get().readObject(new ByteBufferInput(data), String.class);
            LOGGER.debug("KafkaKryoStringSerDe.deserialize time: {}", String.valueOf(System.nanoTime() - time));
            return value;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error reading bytes", e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, String data) {
        long time = System.nanoTime();
        ByteBufferOutput output = new ByteBufferOutput(data.length() + 1);
        kryos.get().writeObject(output, data);

        LOGGER.debug("KafkaKryoStringSerDe.serialize time: {}", String.valueOf(System.nanoTime() - time));
        return output.toBytes();
    }

    @Override
    public void close() {
    }
}
