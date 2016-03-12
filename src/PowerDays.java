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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class PowerDays {

   public static class PowerMapper     // Need to replace the four type labels there with actual Java class names
         extends Mapper<LongWritable, Text, Text, Text> {

      @Override   // we are overriding Mapper's map() method
// map methods takes three input parameters
// first parameter: input key
// second parameter: input value
// third parameter: container for emitting output key-value pairs

      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         if(!(value.toString().charAt(0) == 'D')) {
            String data[] = value.toString().split(";");
            String date[] = data[0].split("/");
            context.write(new Text(date[2]), value);
         }
      }
   }

   public static class PowerReducer   // needs to replace the four type labels with actual Java class names
         extends Reducer<Text, Text, Text, Text> {
      // note: InValueType is a type of a single value Reducer will work with
      //       the parameter to reduce() method will be Iterable<InValueType> - i.e. a list of these values

      @Override  // we are overriding the Reducer's reduce() method
// reduce takes three input parameters
// first parameter: input key
// second parameter: a list of values associated with the key
// third parameter: container  for emitting output key-value pairs
      public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
         HashMap<String, Double> arr = new HashMap<>(); //KV pair for each date

         for (Text val : values) {
            String data[] = val.toString().split(";");
            if (arr.containsKey(data[0])) {
               arr.put(data[0], arr.get(data[0]) + Double.parseDouble(data[2]));
            } else {
               arr.put(data[0], Double.parseDouble(data[2]));
            }
         }

         Set<String> keySet = arr.keySet();
         Iterator<String> iter = keySet.iterator();
         String highestPowerDate = "";
         String highestPowerConsumption = "-1.0";

         while(iter.hasNext()) {
            String date = iter.next();
            Double consumption = arr.get(date);
            if(consumption > Double.parseDouble(highestPowerConsumption)) {
               highestPowerConsumption = consumption + "";
               highestPowerDate = date;
            }
         }

         context.write(new Text("Year: " + key), new Text("Date: " + highestPowerDate + ", Power Consumption: " + highestPowerConsumption));
      }
   }

   public static void main(String[] args) throws Exception {
      // step 1: get a new MapReduce Job object
      Job job = Job.getInstance();  //  job = new Job() is now deprecated

      // step 2: register the MapReduce class
      job.setJarByClass(PowerDays.class);

      //  step 3:  Set Input and Output files
      FileInputFormat.addInputPath(job, new Path("/datasets/household_power_consumption.txt")); // put what you need as input file
      FileOutputFormat.setOutputPath(job, new Path(args[0])); // put what you need as output file

      // step 4:  Register mapper and reducer
      job.setMapperClass(PowerMapper.class);
      job.setReducerClass(PowerReducer.class);

      //  step 5: Set up output information
      job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
      job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

      // step 6: Set up other job parameters at will
      job.setJobName("PowerDays");

      // step 7:  ?
      // step 8: profit
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}