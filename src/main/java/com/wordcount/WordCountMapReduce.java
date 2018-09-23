package com.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountMapReduce {
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    JobConf jobConf = new JobConf();
    Job job = Job.getInstance(jobConf, "wordcount");
    job.setJarByClass(WordCountMapReduce.class);
    job.setMapperClass(WordcountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    boolean b = job.waitForCompletion(true);
    if (!b) {
      System.out.println("wordcount task fail!");
    }
  }
}
