package com.nm.htm.serde;

import com.nm.htm.htm.HTMNetwork;
import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.kafka.MonitoringRecordState;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import org.apache.spark.serializer.KryoRegistrator;
import org.joda.time.DateTime;
import org.numenta.nupic.network.Network;
import org.numenta.nupic.serialize.SerialConfig;

import java.lang.invoke.SerializedLambda;

public class SparkKryoHTMRegistrator implements KryoRegistrator {

    public SparkKryoHTMRegistrator() {
    }

    @Override
    public void registerClasses(Kryo kryo) {

        // we should register the top level classes with kryo
        kryo.register(HTMNetwork.class);
        kryo.register(MonitoringRecord.class);
        kryo.register(Network.class);
        kryo.register(MonitoringRecordState.class);
        for (Class c : SerialConfig.DEFAULT_REGISTERED_TYPES)
            kryo.register(c);

        kryo.register(org.apache.spark.streaming.rdd.MapWithStateRDDRecord.class);
        kryo.register(org.apache.spark.streaming.util.OpenHashMapBasedStateMap.class);
        kryo.register(org.apache.spark.streaming.util.OpenHashMapBasedStateMap.StateInfo.class);

        kryo.register(int[][].class);
        kryo.register(byte[][].class);
        kryo.register(java.lang.Object.class);
        kryo.register(java.lang.Object[].class);
        kryo.register(java.lang.Class.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(java.util.HashSet.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.LinkedHashMap.class);
        kryo.register(java.util.LinkedHashSet.class);
        kryo.register(java.util.concurrent.atomic.AtomicLong.class);

        kryo.register(org.numenta.nupic.encoders.MultiEncoder.class);
        kryo.register(org.numenta.nupic.encoders.DateEncoder.class);
        kryo.register(org.numenta.nupic.encoders.ScalarEncoder.class);
        kryo.register(org.numenta.nupic.encoders.EncoderTuple.class);
        kryo.register(org.numenta.nupic.algorithms.TemporalMemory.class);
        kryo.register(org.numenta.nupic.algorithms.SpatialPooler.class);
        kryo.register(org.numenta.nupic.model.Connections.class);
        kryo.register(org.numenta.nupic.model.Connections.Activity.class);
        kryo.register(org.numenta.nupic.model.Cell[].class);
        kryo.register(org.numenta.nupic.util.Topology.class);
        kryo.register(org.numenta.nupic.util.SparseBinaryMatrix.class);
        kryo.register(org.numenta.nupic.util.SparseObjectMatrix.class);
        kryo.register(org.numenta.nupic.util.MersenneTwister.class);

        kryo.register(gnu.trove.map.hash.TIntObjectHashMap.class);
        kryo.register(gnu.trove.set.hash.TIntHashSet.class);
        kryo.register(gnu.trove.list.array.TIntArrayList.class);

        try {
            kryo.register(load("scala.reflect.ManifestFactory$$anon$2"));
            kryo.register(load("gnu.trove.map.hash.TIntObjectHashMap$1"));
            kryo.register(load("org.numenta.nupic.algorithms.Anomaly$1"));
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        kryo.register(SerializedLambda.class);
        kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
        kryo.register(DateTime.class, new JodaDateTimeSerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);
    }

    private Class<?> load(String name) throws ClassNotFoundException {
        return Class.forName(name, false, getClass().getClassLoader());
    }
}