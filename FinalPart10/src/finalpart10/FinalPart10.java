/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart10;

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
public class FinalPart10 {

    /**
     * @param args the command line arguments
     */
    public static class MinMaxMapper extends Mapper<Object, Text, Text, StatusTupleClass> {

        private Text company = new Text();
        private StatusTupleClass result = new StatusTupleClass();
        
        

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                System.out.println(se[1]);
                System.out.println(se[7]);
                company.set(se[1]);
              
                if(se[15].equalsIgnoreCase("Closed") || se[15].equalsIgnoreCase("Closed with explanation"))
                        {
                            result.setClosedCount(1);
                        }
                else if(se[15].equalsIgnoreCase("Closed with monetary relief"))
                {
                    result.setMonCount(1);
                    
                }
                else if(se[15].equalsIgnoreCase("Closed with non-monetary relief"))
                {
                    result.setNonMonCount(1);
                    
                }
                else if(se[15].equalsIgnoreCase("Untimely response"))
                {
                    result.setUntimelyCount(1);
                    
                }
                
                result.setCount(1);
                context.write(company, result);

            }

        }
    }

    public static class MinMaxReducer extends Reducer<Text, StatusTupleClass, Text, StatusTupleClass> {

        private StatusTupleClass result = new StatusTupleClass();

        public void reduce(Text key, Iterable<StatusTupleClass> values, Context context) throws IOException, InterruptedException {

            result.setUntimelyCount(0);
            result.setClosedCount(0);
            result.setMonCount(0);
            result.setNonMonCount(0);
            result.setCount(0);
            long sum=0;
            long closed=0;
            long mon=0;
            long nonMon=0;
            long untimely=0;
            //result.setStockAdjClose(0);

            for (StatusTupleClass value : values) {
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
                  closed=closed + value.getClosedCount();
                  mon=mon + value.getMonCount();
                  nonMon= nonMon + value.getNonMonCount();
                  untimely=untimely+value.getUntimelyCount();

            }
                 result.setCount(sum);
                 result.setClosedCount(closed);
                 
                 result.setMonCount(mon);
                 result.setNonMonCount(nonMon);
                 result.setUntimelyCount(untimely);
                 context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockVolume Min Max date");
        job.setJarByClass(FinalPart10.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StatusTupleClass.class);
        //job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StatusTupleClass.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    
}
