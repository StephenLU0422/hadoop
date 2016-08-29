package cn.project.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficSumApp {
	public static class TrafficMapper extends Mapper<LongWritable, Text, Text, TrafficWritable>{
		
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
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(this.upPackNum);
			out.writeLong(this.downPackNum);
			out.writeLong(this.upPayLoad);
			out.writeLong(this.downPayLoad);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			
		}
		
		
	}

}
