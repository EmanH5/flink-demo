package com.globomantics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

public class UniqueAdClickAggregator {

  private static Properties ccloudProps() {
    String bootstrap = getenvOrThrow("CCLOUD_BOOTSTRAP");
    String apiKey    = getenvOrThrow("CCLOUD_API_KEY");
    String apiSecret = getenvOrThrow("CCLOUD_API_SECRET");

    Properties p = new Properties();
    p.setProperty("bootstrap.servers", bootstrap);
    p.setProperty("security.protocol", "SASL_SSL");
    p.setProperty("sasl.mechanism", "PLAIN");
    p.setProperty(
        "sasl.jaas.config",
        String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
            apiKey, apiSecret));
    p.setProperty("ssl.endpoint.identification.algorithm", "https");
    return p;
  }

  private static String getenvOrThrow(String k) {
    String v = System.getenv(k);
    if (v == null || v.isEmpty()) throw new IllegalStateException("Missing env: " + k);
    return v;
  }

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10_000);

    Properties sec = ccloudProps();

    // Source: Kafka (ad-clicks)
    KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(sec.getProperty("bootstrap.servers"))
        .setTopics("ad-clicks")
        .setGroupId("globomantics-unique-clicks")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(new SimpleStringSchema())
        .setProperty("security.protocol", sec.getProperty("security.protocol"))
        .setProperty("sasl.mechanism", sec.getProperty("sasl.mechanism"))
        .setProperty("sasl.jaas.config", sec.getProperty("sasl.jaas.config"))
        .setProperty("ssl.endpoint.identification.algorithm", sec.getProperty("ssl.endpoint.identification.algorithm"))
        .build();

    ObjectMapper MAPPER = new ObjectMapper();

    DataStream<ClickEvent> events = env
        .fromSource(source, WatermarkStrategy.noWatermarks(), "ad-clicks")
        .map(line -> {
          JsonNode n = MAPPER.readTree(line);
          return new ClickEvent(
              n.get("adId").asText(),
              n.get("userId").asText(),
              n.get("ts").asLong()
          );
        })
        .returns(ClickEvent.class)
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((e, ts) -> e.getTs())
        );

    // Deduplicate per adId/userId (global-first)
    DataStream<ClickEvent> unique = events
        .keyBy(ClickEvent::getAdId)
        .process(new UniqueUserDeduplicator());

    // Sliding window: 1 min size, 10s slide
    DataStream<Tuple2<String, Long>> perAdCounts = unique
        .keyBy(ClickEvent::getAdId)
        .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
        .process(new ProcessWindowFunction<ClickEvent, Tuple2<String, Long>, String, TimeWindow>() {
          @Override
          public void process(String adId, Context ctx, Iterable<ClickEvent> it, Collector<Tuple2<String, Long>> out) {
            long c = 0L; for (ClickEvent ignored : it) c++;
            out.collect(Tuple2.of(adId, c));
          }
        });

    // Sink: Kafka (ad-agg) with exactly-once
    KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(sec.getProperty("bootstrap.servers"))
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic("ad-agg")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
        )
        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
        .setTransactionalIdPrefix("globomantics-uniqagg-")
        .setProperty("security.protocol", sec.getProperty("security.protocol"))
        .setProperty("sasl.mechanism", sec.getProperty("sasl.mechanism"))
        .setProperty("sasl.jaas.config", sec.getProperty("sasl.jaas.config"))
        .setProperty("ssl.endpoint.identification.algorithm", sec.getProperty("ssl.endpoint.identification.algorithm"))
        .build();

    perAdCounts
        .map(t -> t.f0 + "," + t.f1)
        .returns(Types.STRING)
        .sinkTo(sink);

    env.execute("Globomantics-Unique-Ad-Clicks");
  }
}
