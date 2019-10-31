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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PRNodeWritable implements Writable{
    private int id;
    private double pageRank;
    private List<Integer> adjList;

    public PRNodeWritable(){
        adjList = new ArrayList<Integer>();
    }
    public PRNodeWritable(int id) {
        this();
        this.id = id;
    }
    public PRNodeWritable(int id, double page) {
        this(id);
        this.pageRank = page;
    }
    public PRNodeWritable(PRNodeWritable other) {   // copy constructor from other
        this(other.getID(), other.getPageRank());
        this.addAllToList(other.getAdj());
    }
    public int getID(){
        return this.id;
    }
    public double getPageRank(){
        return this.pageRank;
    }
    public List<Integer> getAdj(){
        return this.adjList;
    }
    public void setId(int id){
        this.id = id;
    }
    public void setPageRank(double p){
        this.pageRank = p;
    }
    public void addToList(int k){
        adjList.add(k);
    }
    public void addAllToList(List l) {
        adjList.addAll(l);
    }


    public void write(DataOutput out) throws IOException {
        // Writing format: <id> <pageRank> <size (n)> <k1><v1>....<kn><vn> 
        out.writeInt(id);
        out.writeDouble(pageRank);
        out.writeInt(adjList.size());
        
        for(Integer x: adjList) {
            out.writeInt(x);
        }
 
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        pageRank = in.readDouble();
        int size = in.readInt();
        adjList = new ArrayList<Integer>();
        for(; size > 0; size--) {
            adjList.add(in.readInt());
        }
    }

    @Override
    public String toString() {
        String s = "";
        s += this.id + " " + this.pageRank + " ";
        for(Integer x: adjList){
            s += x + " ";
        }
        return s;
    }


}