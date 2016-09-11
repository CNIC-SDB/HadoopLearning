package cn.la.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text word=new Text();
		//因为若每个单词出现后，就置为1，并将其作为一个<key,value>对，因此可以声明为常量，值为1
		private final static IntWritable one=new IntWritable(1);
		@Override
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String lineValue=value.toString();
			StringTokenizer tokenizer=new StringTokenizer(lineValue);
			while (tokenizer.hasMoreTokens()) {
				String wordValue = (String) tokenizer.nextToken();
				word.set(wordValue);
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result=new IntWritable();
		
		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable val:values){
				sum+=val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		boolean isSuccess=job.waitForCompletion(true);
	}
}
