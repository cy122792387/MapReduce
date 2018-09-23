package com.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
  private long upFlow;
  private long dFlow;
  private long sumFlow;

  public FlowBean() {
  }

  public FlowBean(Long upFlow, Long dFlow) {
    this.upFlow = upFlow;
    this.dFlow = dFlow;
    sumFlow = upFlow + dFlow;
  }

  public void write(DataOutput out) throws IOException {
    out.writeLong(upFlow);
    out.writeLong(dFlow);
    out.writeLong(sumFlow);
  }

  @Override
  public String toString() {
    return
        "upFlow=" + upFlow +
            ", dFlow=" + dFlow +
            ", sumFlow=" + sumFlow;
  }

  public void readFields(DataInput in) throws IOException {
    upFlow = in.readLong();
    dFlow = in.readLong();
    sumFlow = in.readLong();
  }

  public long getUpFlow() {
    return upFlow;
  }

  public void setUpFlow(long upFlow) {
    this.upFlow = upFlow;
  }

  public long getdFlow() {
    return dFlow;
  }

  public void setdFlow(long dFlow) {
    this.dFlow = dFlow;
  }

  public long getSumFlow() {
    return sumFlow;
  }

  public void setSumFlow(long sumFlow) {
    this.sumFlow = sumFlow;
  }


}
