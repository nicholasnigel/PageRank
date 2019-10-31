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

public class PageRank {
    public enum massloss{MASS};
    public enum test{TEST};
    public static class PRMapper extends Mapper<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        private int length;
        
        // emit key and node structure, also emit neighboring id with a distributed pagerank value
        public void map(IntWritable key, PRNodeWritable value, Context context) throws IOException , InterruptedException {
            context.write(key, value);  // emit node structure immediately
            
            List<Integer> adjacentList = value.getAdj();
            int length = adjacentList.size();
            double pagerank = value.getPageRank();
            // if length 0, it's dangle node must record how much this is because it is contributing to missing mass value
            
            if(length > 0) {
                // if length > 0 emit to each element in adj list the distribution value
                double distribution = pagerank/length;
                PRNodeWritable emitted = new PRNodeWritable(-1, distribution);      // set id to -1 to identify it as not a node structure
                for(Integer target: adjacentList) {
                    context.write(new IntWritable(target), emitted);    
                }
            }
            else {  // if dangling nodes write 
                context.write(new IntWritable(-1), value); 
            }
            
        }

    }

    public static class PRReducer extends Reducer<IntWritable, PRNodeWritable, IntWritable, PRNodeWritable> {
        public void reduce(IntWritable key, Iterable<PRNodeWritable> values, Context context) throws IOException, InterruptedException {
            
            int id = key.get();
            double sum = 0; //used as sum for incoming pagerank values

            // if id is -1 it is dangling nodes sum up the missing pagerank values
            if(id == -1) {
                for(PRNodeWritable pr: values) {
                    sum += pr.getPageRank();
                }
                long m = Double.doubleToLongBits(sum);
                context.getCounter(massloss.MASS).setValue(m);  // passing data through counter
            }
            else {
                PRNodeWritable node = null;
                // check id 
                for(PRNodeWritable val: values) {
                    
                    if(val.getID() != -1 ) {
                        node = new PRNodeWritable(val);
                    }
                    else if(val.getID() == -1) {
                        sum += val.getPageRank();    
                    }
                }
                node.setPageRank(sum);
                context.write(key, node);   // emit the node structure
            }
        }
    }

    public static Job iteratePageRank(Configuration config, String input, String output) throws Exception  {
        Job job = new Job(config, "PageRank Iteration");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(PageRank.class);
        job.setMapperClass(PRMapper.class);
        job.setReducerClass(PRReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        return job;
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //String input = "/user/hadoop/pr/input";
        //String output = "/user/hadoop/pr/output";

        // Input Parameters
        String input = args[2];
        String output = args[3];
        String intermediatePath = "/user/hadoop/pr/temp";
        String intermediatePath2 = "/user/hadoop/pr/tm";
        double alpha = Double.parseDouble(args[0]);
        int iterations = Integer.parseInt(args[1]);
        FileSystem hdfs = FileSystem.get(conf);
        conf.setDouble("alpha", alpha);
        List<String> temporary = new ArrayList<String>();   // temporary paths to be deleted

        // PreProcessing
        Job preprocess = PRPreProcess.preProcess(conf, input, intermediatePath);
        preprocess.waitForCompletion(true);
        temporary.add(intermediatePath);
        int nodecount = (int) preprocess.getCounters().findCounter(PRPreProcess.NodeCount.COUNT).getValue();    // total number of nodes
        conf.setInt("nodecount", nodecount); // put into configuration now
        
        String prevPath = intermediatePath;
        String nextPath = "";
        int k = 1;
        // Iterating Pagerank
        for(int i = 0 ; i < iterations - 1; i++){
            
            nextPath = intermediatePath + k;   // ...../temp1 ....../temp2 ..
            k++;
            Job pagerank = iteratePageRank(conf, prevPath, nextPath);
            pagerank.waitForCompletion(true);
            temporary.add(nextPath);
            
            long l = pagerank.getCounters().findCounter(massloss.MASS).getValue();
            double loss = Double.longBitsToDouble(l);
            System.out.println(loss);
            conf.setDouble("massloss", loss); // mass loss
            
            prevPath = nextPath;
            nextPath = intermediatePath + k;
            k++;
            // adjustment after pagerank
            Job adjustment = PRAdjust.getAdjustment(conf, prevPath, nextPath);
            adjustment.waitForCompletion(true);
            temporary.add(nextPath);
            prevPath = nextPath;
        
        }

        Job fr = iteratePageRank(conf, prevPath, intermediatePath+0);
        temporary.add(intermediatePath+0);
        fr.waitForCompletion(true);

        long ls = fr.getCounters().findCounter(massloss.MASS).getValue();
        
        conf.setDouble("massloss", Double.longBitsToDouble(ls));

        Job fin = PRAdjust.output(conf, intermediatePath+0, output);
        fin.waitForCompletion(true);

        // Deletion of all Temporary places
        for(String p: temporary) {
            Path x = new Path(p);
            hdfs.delete(x, true);
        }
    }

}