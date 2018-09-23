package com.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

  protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] fileds = line.split("\t");
    String phoneNbr = fileds[0];
    Long upFlow = Long.parseLong(fileds[1]);
    Long dFlow = Long.parseLong(fileds[2]);
    context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));

  }
}
