// Author: Timothy Chu & Michael Wong
// Lab 8
// CPE369 - Section 01

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LargestValue {

   public static class TestMapper     // Need to replace the four type labels there with actual Java class names
         extends Mapper<LongWritable, Text, Text, Text> {

      @Override   // we are overriding Mapper's map() method
// map methods takes three input parameters
// first parameter: input key
// second parameter: input value
// third parameter: container for emitting output key-value pairs

      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         String newVal = "Test," + value.toString();
         context.write(new Text("Hand"), new Text(newVal));
      }
   }

   public static class TrainMapper     // Need to replace the four type labels there with actual Java class names
         extends Mapper<LongWritable, Text, Text, Text> {

      @Override   // we are overriding Mapper's map() method
// map methods takes three input parameters
// first parameter: input key
// second parameter: input value
// third parameter: container for emitting output key-value pairs

      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         String newVal = "Train," + value.toString();
         context.write(new Text("Hand"), new Text(newVal));
      }
   }

   public static class PokerReducer   // needs to replace the four type labels with actual Java class names
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
         Integer TestHighVal = -1;
         Integer TestHighCounter = 1;
         Integer TrainHighVal = -1;
         Integer TrainHighCounter = 1;

         for (Text val : values) {
            String hand[] = val.toString().split(",");
            int handVal = 0;
            handVal += ((hand[2].equals("1")) == true ? 14 : Integer.parseInt(hand[2]));
            handVal += ((hand[4].equals("1")) == true ? 14 : Integer.parseInt(hand[4]));
            handVal += ((hand[6].equals("1")) == true ? 14 : Integer.parseInt(hand[6]));
            handVal += ((hand[8].equals("1")) == true ? 14 : Integer.parseInt(hand[8]));
            handVal += ((hand[10].equals("1")) == true ? 14 : Integer.parseInt(hand[10]));

            if (hand[0].equals("Test")) {
               if (handVal > TestHighVal) {
                  TestHighVal = handVal;
                  TestHighCounter = 1;
               } else if (handVal == TestHighVal) {
                  TestHighCounter++;
               } else {
                  // found lower handVal - nothing to do.
               }
            } else {
               if (handVal > TrainHighVal) {
                  TrainHighVal = handVal;
                  TrainHighCounter = 1;
               } else if (handVal == TrainHighVal) {
                  TrainHighCounter++;
               } else {
                  // found lower handVal - nothing to do.
               }
            }
         }

         if (TestHighVal > TrainHighVal) {
            context.write(new Text("File Name: Test"), new Text("High Value: " + TestHighVal + ", Count: " + TestHighCounter));
         } else if (TestHighVal == TrainHighVal) {
            context.write(new Text("File Name: Test"), new Text("High Value: " + TestHighVal + ", Count: " + TestHighCounter));
            context.write(new Text("File Name: Train"), new Text("High Value: " + TrainHighVal + ", Count: " + TrainHighCounter));
         } else {
            context.write(new Text("File Name: Train"), new Text("High Value: " + TrainHighVal + ", Count: " + TrainHighCounter));
         }
      }
   }

   public static void main(String[] args) throws Exception {
      // step 1: get a new MapReduce Job object
      Job job = Job.getInstance();  //  job = new Job() is now deprecated

      // step 2: register the MapReduce class
      job.setJarByClass(LargestValue.class);

      //  step 3:  Set Input and Output files
      MultipleInputs.addInputPath(job, new Path("/datasets/poker-hand-testing.data.txt"),
            TextInputFormat.class, TestMapper.class); // put what you need as input file
      MultipleInputs.addInputPath(job, new Path("/datasets/poker-hand-traning.true.data.txt"),
            TextInputFormat.class, TrainMapper.class); // put what you need as input file
      FileOutputFormat.setOutputPath(job, new Path(args[0])); // put what you need as output file

      // step 4:  Register reducer
      job.setReducerClass(PokerReducer.class);

      //  step 5: Set up output information
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
      job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

      // step 6: Set up other job parameters at will
      job.setJobName("LargestValue");

      // step 7:  ?
      // step 8: profit
      System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}