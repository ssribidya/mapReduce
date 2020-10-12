package wordCount2;
import java.io.IOException;
import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.spark.SparkConf;

public class WordCount {
	
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();

	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	       String line = value.toString();
	       StringTokenizer tokenizer = new StringTokenizer(line);
	       while (tokenizer.hasMoreTokens()) {
	         word.set(tokenizer.nextToken());
	         output.collect(word, one);
	       }
     }
}
	

public static void main(String[] args) {
		JobConf conf = new JobConf(WordCount.class);
	     conf.setJobName("wordcount");
	
	     conf.setOutputKeyClass(Text.class);
	     conf.setOutputValueClass(IntWritable.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	   }
}

