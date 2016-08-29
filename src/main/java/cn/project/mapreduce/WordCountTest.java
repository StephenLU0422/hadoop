package cn.project.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cn.project.mapreduce.WordCountTest.MyMapper.MyReducer;

public class WordCountTest {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, WordCountTest.class.getSimpleName());
		//设置文件输入路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.43.98:9000/user/helloworld.txt"));
		
		job.setInputFormatClass(TextInputFormat.class);//将文件内容告诉job
		
		job.setMapperClass(MyMapper.class);//将mapper和reduce告诉job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//输出路径
		String OUT_DIR = "hdfs://192.168.43.98:9000/outWCTest2";
		
		FileOutputFormat.setOutputPath(job, new Path(OUT_DIR));
		job.setOutputFormatClass(TextOutputFormat.class);
		job.waitForCompletion(true);//让job执行
	}
	//将输出目录覆盖
	public static void deleteOutDir(Configuration conf, String OUT_DIR) throws IOException, URISyntaxException{
		FileSystem fileSystem = FileSystem.get(new URI(OUT_DIR), conf);
		if (fileSystem.exists(new Path(OUT_DIR))) {
			fileSystem.delete(new Path(OUT_DIR), true);
			
		}
	}
	
		//定义mapper函数类,输入<long偏移量,string文本内容>,输出<k1,v1>
		public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
			//覆盖mapper函数
			Text k2 = new Text();
			LongWritable v2 = new LongWritable();	
			
			@Override
			protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

				//对文本split
				String[] words = value.toString().split(" ");
				//排序后的结果是<hello,1><hello,1><me,1><you,1>/
				for (String word : words) {
					k2.set(word);
					v2.set(1L);
					context.write(k2, v2);
				}
			}
		//定义Reduce函数类
			public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
				//覆盖Reduce函数
				//分组后的结果是相同k2的放到一个集合中<hello,{1,1}><me,{1}><you,{1}>产生3个分组
				LongWritable v3 = new LongWritable();
				@Override
				protected void reduce(Text k2, Iterable<LongWritable> v2s, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
				//写自己reduce
					long count = 0L;
					for (LongWritable v2 : v2s) {
						count+=v2.get();
					}
					v3.set(count);
					context.write(k2,v3);
				}
			}
		}
	
}
