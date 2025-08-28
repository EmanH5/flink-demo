package com.globomantics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.OnCheckpointRollingPolicy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/** Flink 2.0 + Java 17 demo: simulate clicks, dedupe by user, window counts, write to files. */
public class UniqueAdClickAggregator {
  public static void main(String[] args) throws Exception {
    // optional first arg = output directory (default: "out")
    final String outDir = (args != null && args.length > 0) ? args[0] : "out";

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    env.enableCheckpointing(10_000); // finalize file parts on checkpoints

    // ---- Source: synthetic generator ----
    DataStream<ClickEvent> events = env
        .addSource(new ClickEventGenerator())
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(7))
                .withTimestampAssigner((e, ts) -> e.getTs()));

    // ---- Deduplicate users per ad ----
    DataStream<ClickEvent> unique = events
        .keyBy(ClickEvent::getAdId)
        .process(new UniqueUserDeduplicator());

    // ---- Sliding window 1m size, 10s slide ----
    DataStream<Tuple2<String, Long>> perAdCounts = unique
        .keyBy(ClickEvent::getAdId)
        .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
        .process(new ProcessWindowFunction<ClickEvent, Tuple2<String,Long>, String, TimeWindow>() {
          @Override
          public void process(String adId, Context ctx, Iterable<ClickEvent> it,
                              Collector<Tuple2<String, Long>> out) {
            long c = 0L; for (ClickEvent ignored : it) c++;
            out.collect(Tuple2.of(adId, c));
          }
        });

    // ---- Format and write results to files ----
    DataStream<String> lines = perAdCounts
        .map(t -> String.format("UniqueAdCount,ad=%s,count=%d", t.f0, t.f1))
        .returns(Types.STRING);

    FileSink<String> sink = FileSink
        .forRowFormat(new Path(outDir), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(OnCheckpointRollingPolicy.build()) // finalize on each checkpoint
        .build();

    lines.sinkTo(sink).setParallelism(1); // one output file stream

    // (optional) also print to console
    // lines.print("RESULT");

    env.execute("Globomantics-Unique-Ad-Click-Aggregator (Flink 2.0, Simulated → File)");
  }
}
