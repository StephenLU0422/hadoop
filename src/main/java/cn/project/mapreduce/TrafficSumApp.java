package cn.project.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TrafficSumApp {
	//<k1,v1>
	//<k2,v2>是手机号、单次通信的流量
	//<k3,v3>是手机号、流量汇总
	public static void main(String[] args) throws Exception {
		//配置
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, TrafficSumApp.class.getSimpleName());
		//jab包
		job.setJarByClass(TrafficSumApp.class);
		//1.指定数据源
		FileInputFormat.setInputPaths(job, args[0]);
		//2.map
		job.setMapperClass(TrafficMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TrafficWritable.class);
		//3.parttion，使用框架默认，暂时不修改
		//4.sort、group.使用框架默认，不去修改
		//5.归约。使用框架默认的(不使用归约)，不去做修改
		//6.网络传输数据的过程，称作shuffle，是reducer通过HTTP拉数据
		//7.reduce
		job.setReducerClass(TrafficReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TrafficWritable.class);
		//8.output
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//wait
		job.waitForCompletion(true);
		
	}
	public static class TrafficMapper extends Mapper<LongWritable, Text, Text, TrafficWritable>{
		Text k2 = new Text();//phoneNumber
		TrafficWritable v2 = new TrafficWritable();
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, Text, TrafficWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splited = line.split("\t");
			k2.set(splited[1]);
			//上行数据包数量，下行包数量，上行总流量，下行总流量
			v2.set(splited[6], splited[7], splited[8], splited[9]);
			context.write(k2, v2);
		}
	}
	//<k2,v2>是手机号、单次通信的流量
	//<k3,v3>是手机号、流量汇总
	public static class TrafficReducer extends Reducer<Text, TrafficWritable, Text, TrafficWritable>{
		TrafficWritable v3 = new TrafficWritable();
	@Override
	protected void reduce(
			Text k2,
			Iterable<TrafficWritable> v2s,
			Reducer<Text, TrafficWritable, Text, TrafficWritable>.Context context)
			throws IOException, InterruptedException {
			long upPackNum =0L;
			long downPackNum=0L;
			long upPayLoad=0L;
			long downPayLoad=0L;
			for (TrafficWritable v2 : v2s) {
				upPackNum+=v2.getUpPackNum();
				downPackNum+=v2.getDownPackNum();
				upPayLoad+=v2.getUpPayLoad();
				downPayLoad += v2.getDownPayLoad();
				
			}
			v3.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
			context.write(k2, v3);
		}
	}
	
	public static class TrafficWritable implements Writable{
		private long upPackNum;
		private long downPackNum;
		private long upPayLoad;
		private long downPayLoad;
		
		//无参构造
		public TrafficWritable(){
			
		}
		//自定义
		public TrafficWritable(long upPackNum, long downPackNum, long upPayLoad, long downPayLoad){
			super();
			set(upPackNum, downPackNum, upPayLoad,downPayLoad);
		}
		
		public void set(long upPackNum, long downPackNum, long upPayLoad, long downPayLoad) {
			this.upPackNum = upPackNum;
			this.downPackNum = downPackNum;
			this.upPayLoad = upPayLoad;
			this.downPayLoad = downPayLoad;
		}
		//String 转换成long
		public void set(String upPackNum, String downPackNum, String upPayLoad, String downPayLoad) {
			this.upPackNum = Long.parseLong(upPackNum);
			this.downPackNum = Long.parseLong(downPackNum);
			this.upPayLoad = Long.parseLong(upPayLoad);
			this.downPayLoad = Long.parseLong(downPayLoad);
		}
		/**
		 * @return the upPackNum
		 */
		public long getUpPackNum() {
			return upPackNum;
		}
		/**
		 * @return the downPackNum
		 */
		public long getDownPackNum() {
			return downPackNum;
		}
		/**
		 * @return the upPayLoad
		 */
		public long getUpPayLoad() {
			return upPayLoad;
		}
		/**
		 * @return the downPayLoad
		 */
		public long getDownPayLoad() {
			return downPayLoad;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.upPackNum);
			out.writeLong(this.downPackNum);
			out.writeLong(this.upPayLoad);
			out.writeLong(this.downPayLoad);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.upPackNum=in.readLong();
			this.downPackNum=in.readLong();
			this.upPayLoad=in.readLong();
			this.downPayLoad=in.readLong();

		}
		@Override
		public String toString() {
			return upPackNum + "\t" + downPackNum + "\t" + upPayLoad + "\t" + downPayLoad;
		}
		
		
	}

}
