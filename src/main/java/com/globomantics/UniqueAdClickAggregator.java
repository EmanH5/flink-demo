package com.globomantics;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class UniqueUserDeduplicator extends KeyedProcessFunction<String, ClickEvent, ClickEvent> {
  private transient MapState<String, Boolean> seenUsers;

  @Override
  public void open(Configuration parameters) {
    MapStateDescriptor<String, Boolean> desc =
        new MapStateDescriptor<>("seenUsers", Types.STRING, Types.BOOLEAN);

    // (optional) 24h TTL to curb unbounded state growth
    StateTtlConfig ttl = StateTtlConfig
        .newBuilder(Time.days(1))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .build();
    desc.enableTimeToLive(ttl);

    seenUsers = getRuntimeContext().getMapState(desc);
  }

  @Override
  public void processElement(ClickEvent e, Context ctx, Collector<ClickEvent> out) throws Exception {
    if (!seenUsers.contains(e.userId())) {
      seenUsers.put(e.userId(), true);
      out.collect(e); // only first click per (adId,userId)
    }
  }
}
