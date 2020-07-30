import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Probability
{
    public static class CancelMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        // A broken-up binary result for on time (one) and not (0)
        private static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);

        // carrier key
        private final Text carrier = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            // skip header row
            if (key.get() == 0 && value.toString().startsWith("Year,Month,DayofMonth")) {
                return;
            }

            String[] record = value.toString().split(",");

            // the airline carrier
            carrier.set(record[8]);

            // records with NA results are rare, so we exclude
            if (record[14].equals("NA") || record[15].equals("NA")) {
                return;
            }

            // grab delay times
            int arrivalDelay = Integer.parseInt(record[14]);
            int departureDelay = Integer.parseInt(record[15]);

            // no delays in arrival or depature, an onTime flight
            if (arrivalDelay <= 0 && departureDelay <=0) {
                context.write(carrier, one);
                return;
            }

            // there must be a delay on one end or the other
            context.write(carrier, zero);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, FloatWritable>
    {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            float counter = 0;
            for (IntWritable val : values) {
                sum += val.get();
                counter += 1;
            }
            float probability = sum / counter;
            result.set(probability);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Probabilty of delay ");
        job.setJarByClass(Probability.class);
        job.setMapperClass(Probability.CancelMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        // can't have a combiner class in this one
        // job.setCombinerClass(Probability.IntSumReducer.class);
        job.setReducerClass(Probability.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        // set the output key to FloatWritable
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
