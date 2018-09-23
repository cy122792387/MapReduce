package com.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.awt.image.Kernel;
import java.util.HashMap;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

  public static HashMap<String, Integer> provinceDict = new HashMap<>();

  static {
    provinceDict.put("137", 0);
    provinceDict.put("133", 1);
    provinceDict.put("138", 2);
    provinceDict.put("135", 3);
  }

  @Override
  public int getPartition(Text key, FlowBean value, int numPartitions) {
    String prefix = key.toString().substring(0, 3);
    Integer provinced = provinceDict.get(prefix);
    return provinced == null ? 4 : provinced;
  }
}
