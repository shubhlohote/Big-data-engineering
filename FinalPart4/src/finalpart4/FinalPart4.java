/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart4;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
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
public class FinalPart4 {

    /**
     * @param args the command line arguments
     */
    public static class MinMaxMapper extends Mapper<Object, Text, CompositeKeyClass, MinTuple> {

        
        private MinTuple result = new MinTuple();
        private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-");
        Date strDate = new Date();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                try {
                    // System.out.println(se[2]);
                    strDate = format.parse(se[0]);
                    System.out.println(se[0]);
                     System.out.println(se[3]);
                    
                    result.setYear(strDate);
                    result.setCount(1);
                   
                    context.write(new CompositeKeyClass(
							se[1].toString(),
							se[3].toString()), result);
                } catch (ParseException ex) {
                    Logger.getLogger(FinalPart4.class.getName()).log(Level.SEVERE, null, ex);
                }

            }

        }
    }

    public static class MinMaxReducer extends Reducer<CompositeKeyClass, MinTuple, CompositeKeyClass, MinTuple> {

        private MinTuple result = new MinTuple();

        public void reduce(CompositeKeyClass key, Iterable<MinTuple> values, Context context) throws IOException, InterruptedException {

            result.setYear(null);
            result.setCount(0);
            long sum=0;
            //result.setStockAdjClose(0);

            for (MinTuple value : values) {
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
                  result.setYear(value.getYear());
                  sum =sum + value.getCount();

            }
                 result.setCount(sum);
                 context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockVolume Min Max date");
        job.setJarByClass(FinalPart4.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(CompositeKeyClass.class);
        job.setMapOutputValueClass(MinTuple.class);
        //job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(CompositeKeyClass.class);
        job.setOutputValueClass(MinTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

