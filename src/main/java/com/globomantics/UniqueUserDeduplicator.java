package com.globomantics;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
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

    // Optional TTL to curb unbounded growth
    StateTtlConfig ttl = StateTtlConfig
        .newBuilder(Time.days(1))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .build();
    desc.enableTimeToLive(ttl);

    seenUsers = getRuntimeContext().getMapState(desc);
  }

  @Override
  public void processElement(ClickEvent e, Context ctx, Collector<ClickEvent> out) throws Exception {
    final String user = e.getUserId();
    if (!seenUsers.contains(user)) {
      seenUsers.put(user, true);
      out.collect(e); // first click per (adId,userId)
    }
  }
}
