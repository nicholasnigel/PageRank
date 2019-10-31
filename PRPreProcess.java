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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


public class PRPreProcess {
    public enum NodeCount{COUNT};
    

    public static Job preProcess(Configuration config, String input, String output) throws Exception {
        Job job = new Job(config, "Preprocess");
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(PreMapper.class);
        job.setReducerClass(PreReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(PRNodeWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        return job;
    }
    
    public static class PreMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private int nodeID;
        private List<String> line;
        private static List<Integer> nodeids;
        private HashMap<Integer, List> list; 

        protected void setup(Context context) throws IOException, InterruptedException {
            line = new ArrayList<String>();
            list = new HashMap<Integer, List>();
            nodeids = new ArrayList<Integer>();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            String[] tokens = content.split("\\s+");
            for(int i = 0; i < tokens.length; i++){
                if(tokens[i].isEmpty()) continue; //skip white spaces
                line.add(tokens[i]);
                if(line.size() < 3 ) continue;  // fill up until maxes out

                String idString = line.get(0);
                String neighboridString = line.get(1);
                // ignore the 3rd part
                int id = Integer.parseInt(idString);
                int neighborid = Integer.parseInt(neighboridString);

                if(!nodeids.contains(id)){
                    context.getCounter(NodeCount.COUNT).increment(1);
                    nodeids.add(id);
                }
                if(!nodeids.contains(neighborid)){
                    context.getCounter(NodeCount.COUNT).increment(1);
                    nodeids.add(neighborid);
                    context.write(new IntWritable(neighborid), new IntWritable(-1));    // need to create this too to make sure
                }

                List<Integer> adjacencylist;
                if(list.containsKey(new Integer(id))) adjacencylist = list.get(new Integer(id));
                else {
                    adjacencylist = new ArrayList<Integer>();
                    list.put(new Integer(id), adjacencylist);
                }
                adjacencylist.add(new Integer(neighborid));
                line.clear();
            }   
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(Integer key: list.keySet()) {
                IntWritable k = new IntWritable(key);
                List<Integer> neighbors = list.get(key);
                for(Integer n: neighbors) {
                    context.write(k, new IntWritable(n));
                }
            }
        }

    }

    public static class PreReducer extends Reducer<IntWritable, IntWritable, IntWritable, PRNodeWritable> {
        

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            // getting the counter from map and set Initial PageRank Value
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            int totalNodes = (int) currentJob.getCounters().findCounter(NodeCount.COUNT).getValue();
            double initialPR = 1.0/totalNodes;

            List<Integer> adjlist = new ArrayList<Integer>();
            for(IntWritable v: values) {
                if(v.get() == -1) continue;
                adjlist.add(v.get());
            }    
            // create the Node object
            PRNodeWritable node = new PRNodeWritable(key.get());
            node.addAllToList(adjlist);
            node.setPageRank(initialPR);
            context.write(key, node);
        }
    }
    
    /*
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String input = "/user/hadoop/pr/input";
        String output = "/user/hadoop/pr/output";
        
        Job preprocess = preProcess(conf, input, output);
        preprocess.waitForCompletion(true);
        
    }
    */
}