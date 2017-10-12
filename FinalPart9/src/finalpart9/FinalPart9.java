/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart9;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author shubhangi
 */
public class FinalPart9 {

    /**
     * @param args the command line arguments
     */
    public static class StateMapper extends Mapper<Object,Text,Text, Text>{
        private Text outKey=new Text();
         public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
             String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                System.out.println(se[1]);
                System.out.println(se[7]);
                outKey.set(se[2]);
                context.write(outKey, value);
         }
        
    }
    }
         public static class StatePartioner extends Partitioner<Text, Text>
         {

            @Override
            public int getPartition(Text key, Text value, int numReduceTasks) {
               String word = key.toString();
//                char letter = word.toLowerCase().charAt(0);
//		return (int) letter - 97;
//               
                  
                if(word.contains("VA")){
                 
                    return 1 ;
                }else if(word.contains("CA")){
                    return 2;
                }else if(word.contains("NY")){
                    return 3 ;
                }else if(word.contains("GA")){
                    return 4 ;
                }else if(word.contains("CT")){
                    return 5 ;
                }else if(word.contains("TX")){
                    return 6 ;
                }
                
                return 0;
                
               
            }
             
         }
         
         public static class StateReducer extends Reducer<Text, Text, Text, NullWritable>
         {
             public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                 for(Text t:values)
                 {
                     context.write(t, NullWritable.get());
                 }
             }
             
         }
         
        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "state partition");
        job.setJarByClass(FinalPart9.class);
        job.setMapperClass(StateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setPartitionerClass(StatePartioner.class);
        job.setNumReduceTasks(7);
        //job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(StateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    
}

