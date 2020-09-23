package com.nm.htm.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.numenta.nupic.model.Persistable;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.HTMObjectInput;
import org.numenta.nupic.serialize.HTMObjectOutput;
import org.numenta.nupic.serialize.SerialConfig;
import org.numenta.nupic.serialize.SerializerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// SerDe HTM objects using SerializerCore (https://github.com/RuedigerMoeller/fast-serialization).
// The rest HTM.java internal classes support Persistence API (with preSerialize/postDeserialize methods),
// therefore we'll create the serializers which will call the preSerialize/postDeserialize
public class SparkFSTHTMSerializer<T> extends Serializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkFSTHTMSerializer.class);
    private final SerializerCore htmSerializer = new SerializerCore(SerialConfig.DEFAULT_REGISTERED_TYPES);

    public SparkFSTHTMSerializer() {
        htmSerializer.registerClass(Network.class);
    }

    @Override
    public T copy(Kryo kryo, T original) {
        return super.copy(kryo, original);
    }

    @Override
    public void write(Kryo kryo, Output kryoOutput, T t) {
        try {
            if (t instanceof Persistable) {
                ((Persistable) t).preSerialize();
            }

            HTMObjectOutput writer = htmSerializer.getObjectOutput(kryoOutput.getOutputStream());
            writer.writeObject(t, t.getClass());
            writer.flush();
            LOGGER.debug("Serialized instance of {}, wrote {} bytes", t.getClass().getName(), writer.getWritten());
        } catch (IOException e) {
            throw new KryoException("Error during HTMObject serialization in SparkFSTHTMSerializer.", e);
        }
    }

    @Override
    public T read(Kryo kryo, Input kryoInput, Class<T> aClass) {
        try {
            HTMObjectInput reader = htmSerializer.getObjectInput(kryoInput.getInputStream());
            final int availableBytes = reader.available();
            final T t = (T) reader.readObject(aClass);

            if (t instanceof Persistable) {
                ((Persistable) t).postDeSerialize();
            }
            LOGGER.debug("Deserialized instance of {}, read {} bytes, still available {} bytes", aClass.getName(), availableBytes - reader.available(), reader.available());
            return t;
        } catch (Exception e) {
            throw new KryoException("Error during HTMObject deserialization in SparkFSTHTMSerializer.", e);
        }
    }

    public static void registerSerializers(Kryo kryo) {
        kryo.register(Network.class, new SparkFSTHTMSerializer<>());
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES)
            kryo.register(c, new SparkFSTHTMSerializer<>());
    }
}