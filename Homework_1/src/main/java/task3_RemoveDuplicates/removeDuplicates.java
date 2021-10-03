package task3_RemoveDuplicates;

import java.awt.*;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class removeDuplicates {
    public static class removeDupMapper extends Mapper<Object, Text, Text, IntWritable> {
        IntWritable num = new IntWritable();
        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {
            String[] strs = value.toString().split(" ");
            int count = 0;
            for(String str: strs){
                Text word = new Text(str);
                num.set(count);
                context.write(word, num);
                ++count;
            }

        }
    }

    public static class removeDupReducer extends Reducer<Text, IntWritable, IntWritable,Text> {
        public void reduce(Text key, Iterable<IntWritable> num , Context context)
                throws IOException, InterruptedException {
            System.out.println(key);
            for (IntWritable i : num){
                context.write(i,key);
            }

        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.设置HDFS配置信息
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 2.设置MapReduce作业配置信息
        String jobName = "removeDuplicates";				//定义作业名称
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(removeDuplicates.class);			//指定运行时作业类

        job.setMapperClass(removeDupMapper.class);	//指定Mapper类
        job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
        job.setMapOutputValueClass(IntWritable.class);	//设置Mapper输出Value类型

        job.setReducerClass(removeDupReducer.class);	//指定Reducer类
        job.setOutputKeyClass(IntWritable.class);				//设置Reduce输出Key类型
        job.setOutputValueClass(Text.class); 	//设置Reduce输出Value类型


        String dataDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/input/task3";
        String outputDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/output/task3";
        Path inPath = new Path(dataDir);
        Path outPath = new Path(outputDir);
        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        // 4.运行作业
        System.out.println("Job: " + jobName + " is running...");
        if (job.waitForCompletion(true)) {
            // Output file
            System.out.println("success!");
            System.exit(0);
        } else {
            System.out.println("failed!");
            System.exit(1);
        }

    }
}
