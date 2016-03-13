// Author: Timothy Chu & Michael Wong
// Lab 8
// CPE369 - Section 01

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

public class SeedKNN {

  public static class SeedMapper     // Need to replace the four type labels there with actual Java class names
      extends Mapper<LongWritable, Text, Text, Text> {

    @Override   // we are overriding Mapper's map() method
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text("1"), value);
    }
  }


  public static class SeedReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      List<Seed> seeds = new ArrayList<>();
      int idx = 0;
      for (Text val : values) {
        seeds.add(new Seed(idx, val));
        idx++;
      }

      for (int i = 0; i < seeds.size(); i++) {
        Seed curr = seeds.get(i);
        List<Dist> dists = new ArrayList<>();

        for (int j = 0; j < seeds.size(); j++) {
          if (i != j) {
            Seed other = seeds.get(j);
            dists.add(new Dist(Dist.computeDist(curr, other), other.number));
          }
        }

        Collections.sort(dists);
        String value = "";
        for (int k = 0; k < 5; k++) {
          value += "(" + dists.get(k).seed + ", <" + dists.get(k).distance + ">) ";
        }
        context.write(new Text(curr.number + 1 + ""), new Text(value));

      }
    }

    static class Seed {
      int number;
      double[] points = new double[7];


      public Seed(int number, Text val) {
        this.number = number;
        Scanner scanner = new Scanner(val.toString());
        for (int i = 0; i < points.length; i++) {
          points[i] = scanner.nextDouble();
        }
      }
    }

    static class Dist implements Comparable<Dist> {
      double distance;
      int seed;

      public static double computeDist(Seed one, Seed two) {
        double[] p = one.points;
        double[] q = two.points;
        double first = Math.pow((p[0] - q[0]), 2) + Math.pow((p[1] - q[1]), 2) + Math.pow((p[2] - q[2]), 2) +
            Math.pow((p[3] - q[3]), 2) + Math.pow((p[4] - q[4]), 2) + Math.pow((p[5] - q[5]), 2) +
            Math.pow((p[6] - q[6]), 2);
        return Math.sqrt(first);
      }

      public Dist(double distance, int seed) {
        this.distance = distance;
        this.seed = seed;
      }


      @Override
      public int compareTo(Dist o) {
        if (this.distance == o.distance) return 0;
        return this.distance < o.distance ? 1 : -1;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    // step 1: get a new MapReduce Job object
    Job job = Job.getInstance();  //  job = new Job() is now deprecated

    // step 2: register the MapReduce class
    job.setJarByClass(SeedKNN.class);

    //  step 3:  Set Input and Output files
    FileInputFormat.addInputPath(job, new Path("/datasets/seeds_dataset.txt")); // put what you need as input file
    FileOutputFormat.setOutputPath(job, new Path(args[0])); // put what you need as output file

    job.setMapperClass(SeedMapper.class);
    job.setReducerClass(SeedReducer.class);

    //  step 5: Set up output information
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
    job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

    // step 6: Set up other job parameters at will
    job.setJobName("Seed KNN");

    // step 7:  ?
    // step 8: profit
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}