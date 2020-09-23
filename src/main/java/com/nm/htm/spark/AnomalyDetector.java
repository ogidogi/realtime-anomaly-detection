package com.nm.htm.spark;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.CanCommitOffsets;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.nm.htm.htm.MonitoringRecord;
import com.nm.htm.kafka.KafkaHelper;
import com.nm.htm.serde.SparkKryoHTMRegistrator;
import com.nm.htm.utils.GlobalConstants;
import com.nm.htm.utils.PropertiesLoader;
import com.nm.htm.utils.Utils;
import com.nm.htm.utils.functions.MapPairsWithState;
import com.nm.htm.utils.functions.MapPartitionsToPairFunction;
import com.nm.htm.utils.functions.SendUpdatedResultsToKafkaTopic;

public class AnomalyDetector implements GlobalConstants {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetector.class);

    public static void main(String[] args) throws Exception {
        //load a properties file from class path, inside static method
        final Properties applicationProperties = PropertiesLoader.getGlobalProperties();
        if (!applicationProperties.isEmpty()) {
            final String appName = applicationProperties.getProperty(SPARK_APP_NAME_CONFIG);
            final String rawTopicName = applicationProperties.getProperty(KAFKA_RAW_TOPIC_CONFIG);
            final String enrichedTopicName = applicationProperties.getProperty(KAFKA_ENRICHED_TOPIC_CONFIG);
            final String checkpointDir = applicationProperties.getProperty(SPARK_CHECKPOINT_DIR_CONFIG);
            final Duration batchDuration = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_BATCH_DURATION_CONFIG)));
            final Duration checkpointInterval = Duration.apply(Long.parseLong(applicationProperties.getProperty(SPARK_CHECKPOINT_INTERVAL_CONFIG)));
            final Boolean enableCheckOnDateOrder = Boolean.parseBoolean(applicationProperties.getProperty(ENABLE_CHECK_ON_DATE_ORDERING));
            
            SparkConf sparkConf = new SparkConf().
                    setAppName(appName)
                    .setMaster("local[1]") //TODO: Adjust parameters for standard run
                    //.set("spark.streaming.backpressure.enabled", "true")  // investigate if this will influence the next parameter
                    .set("spark.streaming.kafka.maxRatePerPartition", "200") // this is per second, so we have windowsessioninterval = 5 seconds, you will get 120 records
                    .set("spark.streaming.kafka.consumer.cache.enabled", "false")
                    .set(SPARK_KRYO_REGISTRATOR_REQUIRED_CONFIG, "false")
                    .set(SPARK_KRYO_UNSAFE_CONFIG, "true")
                    .set(SPARK_INTERNAL_SERIALIZER_CONFIG, KryoSerializer.class.getName())
                    .set(SPARK_KRYO_REGISTRATOR_CONFIG, SparkKryoHTMRegistrator.class.getName());

            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, batchDuration);
            jssc.checkpoint(checkpointDir);

            Integer partitionsCount = Utils.getPartitionsCountforTopic(rawTopicName);
            if (partitionsCount != null) {
                ConsumerStrategy<String, MonitoringRecord> strategy = KafkaHelper
                        .createKryoConsumerStrategy(rawTopicName);
                JavaInputDStream<ConsumerRecord<String, MonitoringRecord>> kafkaDStream = KafkaUtils
                        .createDirectStream(jssc, LocationStrategies.PreferConsistent(), strategy);
                
                kafkaDStream.mapPartitionsToPair(new MapPartitionsToPairFunction())
                        .mapWithState(StateSpec.function(new MapPairsWithState(enableCheckOnDateOrder))
                                .numPartitions(partitionsCount).partitioner(new HashPartitioner(partitionsCount)))
                        .foreachRDD((JavaRDD<Iterable<ConsumerRecord<String, MonitoringRecord>>> rdd) -> {
                            List<OffsetRange> offsets = rdd.mapPartitions(iterator -> {
                                List<OffsetRange> ranges = new SendUpdatedResultsToKafkaTopic(enrichedTopicName)
                                        .call(iterator);
                                return ranges.iterator();
                            }).collect();
                            ((CanCommitOffsets) kafkaDStream.inputDStream())
                                    .commitAsync(offsets.toArray(new OffsetRange[offsets.size()]));
                        });

                jssc.start();
                jssc.awaitTermination();
            }
        }
    }
}