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
        // 1.??????HDFS????????????
        Configuration conf = new Configuration();
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 2.??????MapReduce??????????????????
        String jobName = "wordCountAverage";				//??????????????????
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(wordCountAverage.class);			//????????????????????????

        job.setMapperClass(wordCountMapper.class);	//??????Mapper???
        job.setMapOutputKeyClass(Text.class);			//??????Mapper??????Key??????
        job.setMapOutputValueClass(IntWritable.class);	//??????Mapper??????Value??????

        job.setReducerClass(wordCountReducer.class);	//??????Reducer???
        job.setOutputKeyClass(Text.class);				//??????Reduce??????Key??????
        job.setOutputValueClass(IntWritable.class); 	//??????Reduce??????Value??????


        // 3.?????????????????????????????????
        String dataDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/input/task3"; // ??????????????????
        String outputDir = "/opt/homebrew/Cellar/hadoop/3.3.1/workspace/output/task3"; // ??????????????????
        Path inPath = new Path(dataDir);
        Path outPath = new Path(outputDir);
        FileInputFormat.setInputPaths(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        // 4.????????????
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
