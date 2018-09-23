package com.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] lines = line.split("\t");
    String src = lines[4];
    String message = src.substring(0, src.lastIndexOf("/")) + "," + lines[3];
    context.write(new Text(message), new IntWritable(1));
  }
}
