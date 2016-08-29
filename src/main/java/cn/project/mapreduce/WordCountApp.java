package cn.project.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountApp {
	public static void main(String[] args) {
		
	}
	//mapper&reducer
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		//泛型，hadoop对应的是Longwritable（起始位置偏移量），Text（内容），<k1,v1>是<0, hello	you>,<10,hello me>
		//k2单词数，v2单词数量
		Text k2 = new Text();
		LongWritable v2 =new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			/*找到单词，对行value进行解析，对value进行split操作，但是split不是java的string，
			所以没有split函数，将Text转换成java的string，调用java的toString方法，再调用split方法，再加个数的分隔符制表符，这时会获得一个返回值，返回值是字符串数组String[] words
	要找到其中的K2，用for循环*/
			String[] words = value.toString().split("\t");
			//word表示每一行中的每个单词，即k2，每一行中的每个单词出现的次数是多少，常数1，构造出K2，v2，new Text（），new Longwritable
			for (String word : words) {
				k2.set(word);
				v2.set(1L);
				context.write(k2, v2);
			}
		}
	}
	//map函数执行完的输出<hello,1><you,1><hello,1><me,1>
	//排序后的结果是<hello,1><hello,1><me,1><you,1>
	//分组后的结果是相同2k的放到一个集合中<hello,{1,1}><me,{1}><you,{1}>产生3个分组
	//<k3,v3>是<hello, 2>,<me, 1>,<you, 1>
	
}
