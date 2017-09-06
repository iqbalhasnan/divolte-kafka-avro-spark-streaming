package net.rekod.app;

import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class DivolteKafkaAvroSparkExample
{
    public static void main( String[] args )
    {
      // Initialize the Spark Context
      SparkConf sparkConf = new SparkConf()
        .setMaster("local[2]")
        .setAppName("kafka-consumer");

      // Interval to stream the value from topic. Poll for records every 1 second.
      JavaStreamingContext ssc = new JavaStreamingContext(
        sparkConf,
        Durations.seconds(1)
      );

      // Kafka consumer configuration
      Map<String, Object> kafkaParams = new HashMap<String, Object>();
      kafkaParams.put("bootstrap.servers", "localhost:9092");
      kafkaParams.put("key.deserializer", StringDeserializer.class);
      kafkaParams.put("value.deserializer", AvroDeserializer.class);
      kafkaParams.put("group.id", "group1");
      kafkaParams.put("auto.offset.reset", "earliest");
      kafkaParams.put("enable.auto.commit", true);

      // Kafka topic
      Collection<String> topics = Arrays.asList("divolte-data");

      // Create direct kafka input Discretized Stream (DStream)
      JavaInputDStream<ConsumerRecord<String, GenericRecord>> stream = KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, GenericRecord>Subscribe(topics, kafkaParams)
      );

      // Iterate Resilient Distributed Datasets (RDD) and print out record
      stream.foreachRDD(rdd -> {
        rdd.foreach(record -> {
          System.out.println(record.value());
          System.out.println(record.value().get("partyId"));
          System.out.println(record.value().get("eventType"));
        });
      });

      ssc.start();
      ssc.awaitTermination();
    }
}
