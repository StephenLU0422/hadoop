package cn.project.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest {
	public static void main(String[] args) throws IOException, URISyntaxException {
		//get() require the object通过get方法获得对象
		FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.43.98:9000"), new Configuration());
		//读方法,获得输入流可读取文件内容
		FSDataInputStream fsis = fileSystem.open(new Path("/user/helloworld.txt"));
		//写出到命令行,用工具类
		IOUtils.copyBytes(fsis, System.out, 1024, true);
		//关闭流
		IOUtils.closeStream(fsis);
		
	}
}
