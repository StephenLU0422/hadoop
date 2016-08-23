package cn.project.zookeeper;
import java.io.IOException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperConnet {
	//connet 
	private static String connectString = "192.168.43.98";
	private static int sessionTimeout = 999999;

	public static void main(String[] args) throws Exception {
			Watcher watcher = new Watcher(){

				public void process(WatchedEvent event) {
					System.out.println("监听到的事件："+event);
				}
				
			};
			final ZooKeeper zooKeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
			System.out.println("获得连接："+zooKeeper);
			//获取值，路径，形参watcher，之前创建的对象就可以传回值，统计信息设定为null
			zooKeeper.setData("/javaConnetTest", "helloworld".getBytes(),-1);//修改值。可以使用-1指定版本号
			final byte[] data = zooKeeper.getData("/javaConnetTest", watcher, null);
			System.out.println("读取的值"+new String(data));//字节数组转换为字符串，new String
			zooKeeper.close();	
	}
}
