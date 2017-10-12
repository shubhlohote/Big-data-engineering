/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart8;

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
public class FinalPart8{ 

public static class MinMaxMapper extends Mapper<Object, Text, Text, ModeSubmissionTuple> {

        private Text company = new Text();
        private ModeSubmissionTuple result = new ModeSubmissionTuple();
        
        

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                System.out.println(se[1]);
                System.out.println(se[8]);
                company.set(se[1]);
              
                if(se[8].equalsIgnoreCase("Phone"))
                        {
                            result.setPhoneCount(1);
                        }
                else if (se[8].equalsIgnoreCase("Referral"))
                {
                    result.setReferralCount(1);
                    
                }
                else if (se[8].equalsIgnoreCase("Web"))
                {
                    result.setWeb(1);
                    
                }
                else if (se[8].equalsIgnoreCase("Postal"))
                {
                    result.setPostalCount(1);
                    
                }
                else if (se[8].equalsIgnoreCase("Mail"))
                {
                    result.setMailCount(1);
                    
                }
                else if (se[8].equalsIgnoreCase("Fax"))
                {
                    result.setFaxCount(1);
                    
                }
                else if (se[8].equalsIgnoreCase("Email"))
                {
                    result.setEmailCount(1);
                    
                }
                
                result.setCount(1);
                context.write(company, result);

            }

        }
    }

    public static class MinMaxReducer extends Reducer<Text, ModeSubmissionTuple, Text, ModeSubmissionTuple> {

        private ModeSubmissionTuple result = new ModeSubmissionTuple();

        public void reduce(Text key, Iterable<ModeSubmissionTuple> values, Context context) throws IOException, InterruptedException {

            result.setPhoneCount(0);
            result.setWeb(0);
            result.setPostalCount(0);
            result.setReferralCount(0);
            result.setFaxCount(0);
            result.setMailCount(0);
            result.setEmailCount(0);
            result.setCount(0);
            long sum=0;
            long phoneSum=0;
            long faxSum=0;
            long emailSum=0;
            long mailSum=0;
            long referralSum=0;
            long postalSum=0;
            long webSum=0;
            
            //result.setStockAdjClose(0);

            for (ModeSubmissionTuple value : values) {
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
                  phoneSum=phoneSum + value.getPhoneCount();
                  postalSum=postalSum + value.getPostalCount();
                  faxSum=faxSum + value.getFaxCount();
                  emailSum=emailSum + value.getEmailCount();
                  mailSum=mailSum + value.getMailCount();
                  referralSum=referralSum + value.getReferralCount();
                  webSum=webSum + value.getWeb();

            }
                 result.setCount(sum);
                 result.setPhoneCount(phoneSum);
                 result.setPostalCount(postalSum);
                 result.setWeb(webSum);
                 result.setReferralCount(referralSum);
                 result.setEmailCount(emailSum);
                 result.setMailCount(mailSum);
                 result.setFaxCount(faxSum);
                 context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockVolume Min Max date");
        job.setJarByClass(FinalPart8.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ModeSubmissionTuple.class);
        //job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ModeSubmissionTuple.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
