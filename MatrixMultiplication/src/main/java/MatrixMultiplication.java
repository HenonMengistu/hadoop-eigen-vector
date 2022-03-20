import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class MatrixMultiplication {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String sep = context.getConfiguration().get("sep");
            String[] values = value.toString().split(sep);
            System.out.println(Arrays.asList(values));
            context.write(new Text(values[0] + "\t" + values[1] + "\t" + values[2]), value);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            float multiple = 1;
            String sep = context.getConfiguration().get("sep");
            for (Text i : values) {
                String[] matrixValues = i.toString().split(sep);
                multiple *= Float.parseFloat(matrixValues[matrixValues.length - 1]);
            }
            context.write(key, new Text(String.valueOf(multiple)));
        }
    }

    public static class Summer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            float multiple = 1;
            String sep = context.getConfiguration().get("sep");
            for (Text i : values) {
                String[] matrixValues = i.toString().split(sep);
                multiple *= Float.parseFloat(matrixValues[matrixValues.length - 1]);
                /**
                 * key, (1, 0.2)
                 * remove("(", "")
                 * remove (")", "")
                 * split(", ")
                 */
            }
            context.write(key, new Text(String.valueOf(multiple)));
        }
    }

    static void cleanOutput() throws IOException {
        String[] cmd = {
                "/bin/sh",
                "-c",
                "rm -rf output"
        };

        Process p = Runtime.getRuntime().exec(cmd);

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String line;

        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Args: " + Arrays.toString(args));
        Configuration conf = new Configuration();
        conf.set("sep", args[2]);
        Job multiplicationJob = Job.getInstance(conf, "Matrix Multiplication");
        multiplicationJob.setJarByClass(MatrixMultiplication.class);
        multiplicationJob.setMapperClass(TokenizerMapper.class);
        multiplicationJob.setReducerClass(IntSumReducer.class);
        multiplicationJob.setOutputKeyClass(Text.class);
        multiplicationJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(multiplicationJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(multiplicationJob, new Path(args[1]));

        multiplicationJob.waitForCompletion(true);

    }
}
