/**
 * 
 */
package cn.la.hadoop.mr;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author liuan
 *
 */
public class KpiBrowserMR extends Configured implements Tool {
	
	public static class KpiBrowserMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one=new IntWritable(1);
		private Text browser=new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			Kpi kpi=Kpi.parser(value.toString());
			if(kpi.isValid()){
				browser.set(kpi.getHttp_user_agent());
				context.write(browser, one);
			}
		}
	}
	public static class KpiBrowserReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable result=new IntWritable();
		
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		JobConf conf=new JobConf();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		conf.setJar("E:\\StudyResource\\Workspace\\HadoopLearning\\wordcount\\target\\wordcount-1.0-SNAPSHOT.jar");
		ToolRunner.run(conf,new KpiBrowserMR(), args);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	public int run(String[] arg0) throws Exception {
		Job job=Job.getInstance(getConf(), "kpi-browser");
		job.setJarByClass(KpiBrowserMR.class);;
		job.setMapperClass(KpiBrowserMapper.class);
		job.setReducerClass(KpiBrowserReducer.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem fileSystem=FileSystem.get(new URI(arg0[0]), getConf());
		Path outPath=new Path(arg0[1]);
		if(fileSystem.exists(outPath))
			fileSystem.delete(outPath, true);
		boolean success=job.waitForCompletion(true);
		
		return 0;
	}

}
