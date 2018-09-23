package com.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
  public void reduce(Text key, Iterable<FlowBean> values, Reducer.Context context) throws IOException, InterruptedException {
    long sum_upFlow = 0;
    long sum_dFlow = 0;
    for (FlowBean bean : values) {
      sum_dFlow += bean.getdFlow();
      sum_upFlow += bean.getUpFlow();
    }
    FlowBean resuleBean = new FlowBean(sum_upFlow, sum_dFlow);
    context.write(key, resuleBean);
  }
}