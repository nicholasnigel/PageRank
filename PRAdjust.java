import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Class to adjust the page rank value ( accounting random jump and the dangling nodes)
 * 
 */
public class PRAdjust {
    public static class adjustMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        
        public void map(IntWritable key, PRNodeWritable value, Context context) throws IOException, InterruptedException {
            
            context.write(key, value);           
        }
    }

    public static class adjustReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            // getting some parameters from the configuration
            
            Configuration conf = context.getConfiguration();
            int nodecount = conf.getInt("nodecount", 0);
            double alpha = conf.getDouble("alpha", 0.0);
            double massloss = conf.getDouble("massloss", 0.0);
            double prevPR, updatedPR;
            prevPR = 0.0;
            double randomjump = alpha * (1.0/ nodecount);   // alpha(1/|G|)
            PRNodeWritable node = null;
            for(PRNodeWritable v: values) {
                node = new PRNodeWritable(v);
            }
            prevPR = node.getPageRank();

            double redistribution = (1 - alpha) * (prevPR + massloss/nodecount);
            updatedPR = randomjump + redistribution;

            node.setPageRank(updatedPR);
            context.write(key, node);
            
        }
    }

    public static Job getAdjustment(Configuration conf, String input, String output) throws Exception{
        Job job = new Job(conf, "Adjust");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(PRAdjust.class);
        job.setMapperClass(adjustMapper.class);
        job.setReducerClass(adjustReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        return job;
    }

    public static Job output(Configuration conf, String input, String output) throws Exception {
        Job job = new Job(conf, "Adjust");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(PRAdjust.class);
        job.setMapperClass(adjustMapper.class);
        job.setReducerClass(adjustReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job;
    }

}