package MapReduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;



public class MapreduceDemo {
	
	public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split(" ");
			for(String word:words)
			{
				context.write(new Text(word),new IntWritable(1));
			}
			
		}
		
	}
	
	public static class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int count =0;
			for(IntWritable val:values){
				count += val.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://192.168.88.130:9000");
		Job job = new Job(conf,"wordCount");
		//����Ҫִ�е���
		job.setJarByClass(MapreduceDemo.class);
		
		//����jobҪִ�е�map��reduce��
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReduce.class);
		
		//ָ��reduce��ɺ��������ݵĸ�ʽ
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.88.130:9000/test/testupload.txt"));
		
		
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.88.130:9000/test/TestMapReduce"));
		
		//���������ɾ��˳���ǰ����
		System.exit(job.waitForCompletion(true)? 0:1);
		
		
		
	}
	
	
	
	
	
	
	
	
	

}
