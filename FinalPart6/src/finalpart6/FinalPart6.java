/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart6;

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
public class FinalPart6 {

    /**
     * @param args the command line arguments
     */
    public static class MinMaxMapper extends Mapper<Object, Text, Text, CompanyStateTuple> {

        private Text state = new Text();
        private CompanyStateTuple result = new CompanyStateTuple();
        
        

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                System.out.println(se[1]);
                System.out.println(se[2]);
                state = new Text(se[2]);
                result.setCompany(se[1]);
                result.setCount(1);
                context.write(state, result);

            }

        }
    }

    public static class MinMaxReducer extends Reducer<Text, CompanyStateTuple, Text, CompanyStateTuple> {

        private CompanyStateTuple result = new CompanyStateTuple();

        public void reduce(Text key, Iterable<CompanyStateTuple> values, Context context) throws IOException, InterruptedException {

            result.setCompany(null);
            result.setCount(0);
            long sum=0;
            //result.setStockAdjClose(0);

            for (CompanyStateTuple value : values) {
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
                  result.setCompany(value.getCompany());
                  sum =sum + value.getCount();

            }
                 result.setCount(sum);
                 context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockVolume Min Max date");
        job.setJarByClass(FinalPart6.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CompanyStateTuple.class);
        //job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompanyStateTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
