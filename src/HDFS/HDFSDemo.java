package HDFS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * java  hdfs ����������
 * ������ hadoop\share\hadoop\common\hadoop-common-2.6.0-cdh5.5.6.jar
 * ������hadoop\share\hadoop\common\lib\�����еİ�
 * ������hadoop\share\hadoop\hdfs\hadoop-hdfs-2.6.0-cdh5.5.6.jar
 * ������hadoop\share\hadoop\hdfs\lib\�����Եİ�
 * 
 * 
 * 
 * @author Administrator
 *
 */

public class HDFSDemo {
	
	public static void main(String[] args) {
		System.out.println("main ����---");
		HDFSDemo demo = new HDFSDemo();
		demo.upload();
	}
	
	/**
	 * ���� �ļ���
	 */
	public void mkdir()
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.mkdirs(new Path("hdfs://192.168.254.131:9000/ZhiChen"));
			System.out.println("�����ɹ�");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * ����
	 */
	public void download()
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.copyToLocalFile(new Path("hdfs://192.168.254.131:9000/README.txt"), new Path("d:/zhicheng.txt") );
			System.out.println("���سɹ�");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * �ϴ�
	 */
	public void upload()
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.254.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(new Path("d:/upload.txt"), new Path("hdfs://192.168.254.131:9000/upload.txt"));
			System.out.println("�ϴ��ɹ�");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	

}
