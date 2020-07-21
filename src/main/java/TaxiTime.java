import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.FloatWritable.Comparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaxiTime {

    public static class TaxiTimeMapper
            extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private final Text airportOut = new Text();
        private final Text airportIn = new Text();
        private final FloatWritable taxiOut = new FloatWritable();
        private final FloatWritable taxiIn = new FloatWritable();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            // skip header row
            if (key.get() == 0 && value.toString().startsWith("Year,Month,DayofMonth")) {
                return;
            }

            String[] record = value.toString().split(",");

            airportOut.set(record[16]);
            airportIn.set(record[17]);
            taxiOut.set(Float.parseFloat(record[20]));
            taxiIn.set(Float.parseFloat(record[19]));
            context.write(airportIn, taxiIn);
            context.write(airportOut, taxiOut);

        }
    }

    public static class AvgReducer
            extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        public void reduce(Text key, Iterable<FloatWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;
            for (FloatWritable val : values) {
                sum += val.get();
                count += 1;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "cancellation reasons");
        job.setJarByClass(SortDescending.class);
        job.setMapperClass(TaxiTimeMapper.class);
        job.setReducerClass(AvgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setSortComparatorClass(Comparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
