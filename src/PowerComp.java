// Author: Timothy Chu
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
import java.util.Scanner;

public class PowerComp {

  public static class PowerMapper     // Need to replace the four type labels there with actual Java class names
      extends Mapper<LongWritable, Text, Text, Text> {

    @Override   // we are overriding Mapper's map() method
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      if (!(value.toString().charAt(0) == 'D') && !(value.toString().contains("?"))) {
        String data[] = value.toString().split(";");
        context.write(new Text(data[0]), value);
      }
    }
  }

  public static class PowerReducer   // needs to replace the four type labels with actual Java class names
      extends Reducer<Text, Text, Text, Text> {

    @Override  // we are overriding the Reducer's reduce() method
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      int count = 0;
      double avgPowerConsumption = 0;
      double sub2 = 0;
      double sub3 = 0;

      for (Text val : values) {
        count++;
        String data[] = val.toString().split(";");
        sub2 += Double.parseDouble(data[7]);
        sub3 += Double.parseDouble(data[8]);
        avgPowerConsumption += Double.parseDouble(data[2]);
      }

      avgPowerConsumption = avgPowerConsumption / count;


      //TODO: Figure out these numbers.
      // Used both
      if (sub2 < 500 || sub3 < 6000) {
        context.write(new Text("true"), new Text(avgPowerConsumption + ""));
      } else {
        context.write(new Text("false"), new Text(avgPowerConsumption + ""));
      }
    }
  }

  public static class SecondMap
      extends Mapper<LongWritable, Text, Text, Text> {

    @Override   // we are overriding Mapper's map() method
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      Scanner scanner = new Scanner(value.toString());
      context.write(new Text(scanner.next()), new Text(scanner.nextDouble() + ""));
    }
  }

  public static class SecondReduce   // needs to replace the four type labels with actual Java class names
      extends Reducer<Text, Text, Text, Text> {

    @Override  // we are overriding the Reducer's reduce() method
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      double average = 0;
      int count = 0;
      for (Text val : values) {
        count++;
        average += Double.parseDouble(val.toString());
      }

      average /= count;
      context.write(key, new Text(average + ""));
    }
  }

  public static class ThirdMap
      extends Mapper<LongWritable, Text, Text, Text> {

    @Override   // we are overriding Mapper's map() method
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text("average"), value);
    }
  }

  public static class ThirdReduce   // needs to replace the four type labels with actual Java class names
      extends Reducer<Text, Text, Text, Text> {

    @Override  // we are overriding the Reducer's reduce() method
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

      double averageTrue = 0;
      double averageFalse = 0;

      for (Text val : values) {
        Scanner scanner = new Scanner(val.toString());
        if (scanner.next().equals("true")) {
          averageTrue = scanner.nextDouble();
        } else {
          averageFalse = scanner.nextDouble();
        }
      }

      context.write(new Text("Average use for both"), new Text(averageTrue + ""));
      context.write(new Text("Average use for none"), new Text(averageFalse + ""));
      context.write(new Text("Difference between both and none"), new Text(averageTrue - averageFalse + ""));
    }
  }

  public static void main(String[] args) throws Exception {
    // First Job
    // step 1: get a new MapReduce Job object
    Job job = Job.getInstance();  //  job = new Job() is now deprecated

    // step 2: register the MapReduce class
    job.setJarByClass(PowerComp.class);

    //  step 3:  Set Input and Output files
    FileInputFormat.addInputPath(job, new Path("/datasets/household_power_consumption.txt")); // put what you need as input file
    FileOutputFormat.setOutputPath(job, new Path("./mwong56/", "output1")); // put what you need as output file

    // step 4:  Register mapper and reducer
    job.setMapperClass(PowerMapper.class);
    job.setReducerClass(PowerReducer.class);

    //  step 5: Set up output information
    job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
    job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

    // step 6: Set up other job parameters at will
    job.setJobName("PowerComp");
    job.waitForCompletion(true);


    // Second job
    job = Job.getInstance();
    job.setJarByClass(PowerComp.class);
    FileInputFormat.addInputPath(job, new Path("./mwong56/output1", "part-r-00000")); // put what you need as input file
    FileOutputFormat.setOutputPath(job, new Path("./mwong56/", "output2")); // put what you need as output file
    job.setMapperClass(SecondMap.class);
    job.setReducerClass(SecondReduce.class);
    job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
    job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
    job.setJobName("Second PowerComp");
    job.waitForCompletion(true);

    // Third job
    job = Job.getInstance();
    job.setJarByClass(PowerComp.class);
    FileInputFormat.addInputPath(job, new Path("./mwong56/output2", "part-r-00000")); // put what you need as input file
    FileOutputFormat.setOutputPath(job, new Path(args[0])); // put what you need as output file
    job.setMapperClass(ThirdMap.class);
    job.setReducerClass(ThirdReduce.class);
    job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
    job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
    job.setJobName("Third PowerComp");
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}