package com.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable> {

  @Override
  public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
    return (orderBean.getItemid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;

  }

}
