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
			
	}
}
