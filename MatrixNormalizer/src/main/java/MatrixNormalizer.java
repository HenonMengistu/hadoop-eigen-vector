
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MatrixNormalizer {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, FloatWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            context.write(new Text("C"), new FloatWritable(Float.parseFloat(values[1].trim())));
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values,
                Context context) throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable i : values) {
                sum +=i.get() * i.get();
            }
            context.write(key, new FloatWritable((float) Math.sqrt(sum)));

        }
    }

    static void cleanOutput() throws IOException {
        String[] cmd = {
                "/bin/sh",
                "-c",
                "rm -rf normalize"
        };

        Process p = Runtime.getRuntime().exec(cmd);

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line;

        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Normalizer");
        job.setJarByClass(MatrixNormalizer.class);
        job.setMapperClass(MatrixNormalizer.TokenizerMapper.class);
        job.setReducerClass(MatrixNormalizer.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        cleanOutput();
        job.waitForCompletion(true);

    }
}
