package it.rome.saiello;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by saiello on 10/25/14.
 */
public class WordCount  extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exit = ToolRunner.run(new WordCount(), args);
        System.exit(exit);
    }


    @Override
    public int run(String[] strings) throws Exception {
        if (strings.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }


        Path input = new Path(strings[0]);
        Path output =new Path(strings[1]);

        Job job = Job.getInstance(getConf());
        job.setJarByClass(WordCount.class);
        FileInputFormat.setInputPaths(job, input);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MyMapper.class);

        FileOutputFormat.setOutputPath(job, output);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();

        private final static IntWritable ONE = new IntWritable(1);


        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString());

            scanner.useDelimiter(" ");

            while(scanner.hasNext()){
                word.set(scanner.next());
                context.write(word, ONE);
            }
            scanner.close();
        }


    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }

    }



}
