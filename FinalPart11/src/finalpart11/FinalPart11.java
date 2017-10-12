/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart11;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
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
public class FinalPart11 { 

public static class MinMaxMapper extends Mapper<Object, Text, Text, Tuple> {

        private Text company = new Text();
        private Tuple result = new Tuple();
        
        

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                System.out.println(se[1]);
                System.out.println(se[7]);
                company.set(se[1]);
              
                if(se[7].equalsIgnoreCase("Yes"))
                        {
                            result.setTimelyCount(1);
                        }
                else
                {
                    result.setTimelyCount(0);
                    
                }
                
                result.setCount(1);
                context.write(company, result);

            }

        }
    }

    public static class MinMaxReducer extends Reducer<Text, Tuple, Text, Tuple> {

        private Tuple result = new Tuple();

        public void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {

            result.setTimelyCount(0);
            result.setCount(0);
            long sum=0;
            long timelySum=0;
            //result.setStockAdjClose(0);

            for (Tuple value : values) {

                  
                  sum =sum + value.getCount();
                  timelySum = timelySum + value.getTimelyCount();

            }
                 result.setCount(sum);
                 result.setTimelyCount(timelySum);
                 context.write(key, result);

        }
    }
    public static class SimplePartitioner extends Partitioner<Text, Tuple>{

    @Override
    public int getPartition(Text key, Tuple value, int numReducerTask) {
        String s= key.toString();
        return (Character.toString(s.charAt(0)).hashCode() % numReducerTask);
    }
    
}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockVolume Min Max date");
        job.setJarByClass(FinalPart11.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Tuple.class);
        job.setCombinerClass(MinMaxReducer.class);
        job.setPartitionerClass(SimplePartitioner.class);
        job.setNumReduceTasks(10);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Tuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
