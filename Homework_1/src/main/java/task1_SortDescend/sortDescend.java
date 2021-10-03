package task1_SortDescend;

import java.io.IOException;

//import task1_MapReduce.sortDescend.SortMapper;
//import task1_MapReduce.sortDescend.SortReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.io.WritableComparator;

public class sortDescend {

    public static class MyComparator extends WritableComparator {
        public MyComparator() {
            // TODO Auto-generated constructor stub
            super(IntWritable.class, true);
        }

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public int compare(WritableComparable a, WritableComparable b) {
            // Default a.compareTo(b), that means when a<b, return -1
            // now it returns 1
            return b.compareTo(a);
        }

        public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
            private IntWritable num = new IntWritable();

            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] strs = value.toString().split(",");

                num.set(Integer.parseInt(strs[1]));

                context.write(num, new Text(strs[0]));
//                System.out.println(num.get() + "," + strs[0]);
            }
        }

        public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

            public void reduce(IntWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                for (Text value : values) {
                    System.out.println(value.toString() + ":" + key.get());
                    context.write(value, key);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");

        String jobName = "sortDescend";
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(sortDescend.class);

        job.setMapperClass(MyComparator.SortMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyComparator.SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(MyComparator.class);

        String dataDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/input/task1";
        String outputDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/output/task1";
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
            System.out.println("success!");
            System.exit(0);
        } else {
            System.out.println("failed!");
            System.exit(1);
        }

    }

}
