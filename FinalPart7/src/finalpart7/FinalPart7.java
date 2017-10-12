/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart7;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author shubhangi
 */
public class FinalPart7{ 

public static class MinMaxMapper extends Mapper<Object, Text, Text, ComplaintResponseTuple> {

        private Text company = new Text();
        private ComplaintResponseTuple result = new ComplaintResponseTuple();
        
        

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

    public static class MinMaxReducer extends Reducer<Text, ComplaintResponseTuple, Text, ComplaintResponseTuple> {

        private ComplaintResponseTuple result = new ComplaintResponseTuple();

        public void reduce(Text key, Iterable<ComplaintResponseTuple> values, Context context) throws IOException, InterruptedException {

            result.setTimelyCount(0);
            result.setCount(0);
            long sum=0;
            long timelySum=0;
            //result.setStockAdjClose(0);

            for (ComplaintResponseTuple value : values) {
//                if (value.getStockVolume() < result.getStockVolume()) {
//                    result.setMin(value.getMin());
//                    result.setStockVolume(value.getStockVolume());
//                    //result.setStockAdjClose(value.getStockAdjClose());
//                }
//                if (value.getStockVolume() > result.getStockVolume()) {
//                    result.setMax(value.getMax());
//                    result.setStockVolume(value.getStockVolume());
//
//                }
//                if(value.getStockAdjClose() > result.getStockAdjClose()){
//                    result.setStockAdjClose(value.getStockAdjClose());
//                }
                  
                  sum =sum + value.getCount();
                  timelySum=timelySum + value.getTimelyCount();

            }
                 result.setCount(sum);
                 result.setTimelyCount(timelySum);
                 context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockVolume Min Max date");
        job.setJarByClass(FinalPart7.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ComplaintResponseTuple.class);
        //job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ComplaintResponseTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
