import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.FileAppender;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MRtest{
	private static Logger logger = Logger.getLogger(MRtest.class);

	public static class TempMap extends Mapper<LongWritable,Text,Text,IntWritable>{//<keyin,valuein,keyout,valueout>
		public void map(LongWritable key, Text value, Context context) throws
			IOException, InterruptedException{
				String line = value.toString();
				String year = line.substring(0,4);
				int temperature = Integer.parseInt(line.substring(8));
				context.write(new Text(year), new IntWritable(temperature));
			}
	}

	public static class TempReduce extends Reducer<Text, IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException,InterruptedException{
			int maxValue = Integer.MIN_VALUE;
			StringBuffer sb = new StringBuffer();
			for(IntWritable value : values){
				maxValue = Math.max(maxValue, value.get());
				sb.append(value).append(",");
			}
			context.write(key, new IntWritable(maxValue));
		}
	}

	public static void main(String[] args) throws Exception{
		String dst = "hdfs://namenode:9000/user/input";
		String dstout = "hdfs://namenode:9000/user/output";
		Configuration conf = new Configuration();

        Job job = new Job(conf,"MRtest");
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstout));
        job.setJarByClass(MRtest.class);
        job.setMapperClass(TempMap.class);
        job.setReducerClass(TempReduce.class);
        job.setCombinerClass(TempReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
        SimpleLayout layout = new SimpleLayout();
        FileAppender appender = null;
        try{
        	appender = new FileAppender(layout,"output.txt",false);
        }catch(Exception e){}
        logger.addAppender(appender);
        logger.setLevel((Level)Level.WARN);//选择不同的日志级别，不同级别影响输出
        // 记录debug级别的信息  
        logger.debug("This is debug message.");  
        // 记录info级别的信息  
        logger.info("This is info message.");  
        // 记录error级别的信息  
        logger.error("This is error message.");
        System.out.println("Finished");

	}


}
