package HDFS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * java  hdfs 基本操作：
 * 包导入 hadoop\share\hadoop\common\hadoop-common-2.6.0-cdh5.5.6.jar
 * 包导入hadoop\share\hadoop\common\lib\下所有的包
 * 包导入hadoop\share\hadoop\hdfs\hadoop-hdfs-2.6.0-cdh5.5.6.jar
 * 包导入hadoop\share\hadoop\hdfs\lib\下所以的包
 * 
 * 
 * 
 * @author Administrator
 *
 */

public class HDFSDemo {
	
	public static void main(String[] args) {
		System.out.println("main 函数---");
		HDFSDemo demo = new HDFSDemo();
		demo.upload();
	}
	
	/**
	 * 创建 文件夹
	 */
	public void mkdir()
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.mkdirs(new Path("hdfs://192.168.254.131:9000/ZhiChen"));
			System.out.println("创建成功");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 下载
	 */
	public void download()
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.copyToLocalFile(new Path("hdfs://192.168.254.131:9000/README.txt"), new Path("d:/zhicheng.txt") );
			System.out.println("下载成功");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 上传
	 */
	public void upload()
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(new Path("d:/upload.txt"), new Path("hdfs://192.168.254.131:9000/upload.txt"));
			System.out.println("上传成功");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

}
