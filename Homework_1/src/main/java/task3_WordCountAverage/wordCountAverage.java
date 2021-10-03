package task3_WordCountAverage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import task3_RemoveDuplicates.removeDuplicates;

import java.io.IOException;
import java.util.StringTokenizer;

public class wordCountAverage {
    static enum CountersEnum {
        INPUT_WORDS
    }
    public static class wordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        IntWritable num = new IntWritable(1);
        public void map(Object key, Text value, Context context )
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                Text word = new Text();
                word.set(itr.nextToken());
                context.write(word, num);
                Counter counter = context.getCounter(CountersEnum.class.getName(), CountersEnum.INPUT_WORDS.toString());
//                System.out.println(counter.getValue());
                counter.increment(1);
            }

        }
    }
    public static class wordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            if(result.get()>1){
                System.out.println(sum);
                context.write(word, result);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.设置HDFS配置信息
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 2.设置MapReduce作业配置信息
        String jobName = "wordCountAverage";				//定义作业名称
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(wordCountAverage.class);			//指定运行时作业类

        job.setMapperClass(wordCountMapper.class);	//指定Mapper类
        job.setMapOutputKeyClass(Text.class);			//设置Mapper输出Key类型
        job.setMapOutputValueClass(IntWritable.class);	//设置Mapper输出Value类型

        job.setReducerClass(wordCountReducer.class);	//指定Reducer类
        job.setOutputKeyClass(Text.class);				//设置Reduce输出Key类型
        job.setOutputValueClass(IntWritable.class); 	//设置Reduce输出Value类型


        // 3.设置作业输入和输出路径
        String dataDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/input/task3"; // 实验数据目录
        String outputDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/output/task3"; // 实验输出目录
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
