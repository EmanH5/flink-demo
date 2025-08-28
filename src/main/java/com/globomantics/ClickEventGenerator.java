package com.globomantics;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/** Synthetic stream source: emits ClickEvent with duplicates + out-of-order timestamps. */
public class ClickEventGenerator implements ParallelSourceFunction<ClickEvent> {
  private volatile boolean running = true;

  @Override
  public void run(SourceContext<ClickEvent> ctx) throws Exception {
    Random rnd = new Random();
    String[] ads = {"A1","A2","A3","A4"};
    int users = 60;
    long maxOutOfOrderMs = 7000;

    while (running) {
      String adId = ads[rnd.nextInt(ads.length)];
      String userId = "U" + (1 + rnd.nextInt(users));
      long now = System.currentTimeMillis();
      long ts = now - rnd.nextInt((int) maxOutOfOrderMs); // some out-of-order

      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(new ClickEvent(adId, userId, ts));
        // occasionally duplicate (adId,userId)
        if (rnd.nextDouble() < 0.35) {
          ctx.collect(new ClickEvent(adId, userId, ts + rnd.nextInt(2000)));
        }
      }
      Thread.sleep(100); // ~10 events/sec per parallel source subtask
    }
  }

  @Override
  public void cancel() { running = false; }
}
