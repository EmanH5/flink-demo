package com.globomantics;

import java.io.Serializable;

public class ClickEvent implements Serializable {
  private String adId;
  private String userId;
  private long ts;

  public ClickEvent() {}

  public ClickEvent(String adId, String userId, long ts) {
    this.adId = adId; this.userId = userId; this.ts = ts;
  }

  public String getAdId() { return adId; }
  public void setAdId(String adId) { this.adId = adId; }
  public String getUserId() { return userId; }
  public void setUserId(String userId) { this.userId = userId; }
  public long getTs() { return ts; }
  public void setTs(long ts) { this.ts = ts; }
}
