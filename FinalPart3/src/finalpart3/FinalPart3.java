/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart3;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.io.IOException;  
import java.nio.ByteBuffer;
import java.util.*;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.conf.*;  
import org.apache.hadoop.io.*;  
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.regex.Matcher;

import java.util.regex.Pattern;
import static javax.management.Query.value;

/**
 *
 * @author shubhangi
 */
public class FinalPart3 {

    /**
     * @param args the command line arguments
     */
   public static class IpMapper extends MapReduceBase 
                        implements Mapper<LongWritable, Text, Text, FloatWritable>
{

  

  // Reusable IntWritable for the count
    private final static FloatWritable one = new FloatWritable(1);
    
    private Text product = new Text();
  
  public void map(LongWritable fileOffset, Text lineContents,
      OutputCollector<Text, FloatWritable> output, Reporter reporter)
      throws IOException {
    
    // apply the regex to the line of the access log
    String line = lineContents.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                System.out.println(se[3]);
               // System.out.println(se[2]);
                product = new Text(se[3]);
                
                output.collect(product, one);

            }
         
        
       
        
        
  }

}
public static class IpReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> 
{

  public void reduce(Text ip, Iterator<FloatWritable> counts,
      OutputCollector<Text, FloatWritable> output, Reporter reporter)
      throws IOException {
    
    int totalCount = 0;
    
    // loop over the count and tally it up
    while (counts.hasNext())
    {
      FloatWritable count = counts.next();
      totalCount += count.get();
    }

output.collect(ip, new FloatWritable(totalCount));

    
    
  }

}

public static class IpMapper1 extends MapReduceBase 
                        implements Mapper<LongWritable, Text, FloatWritable, Text>
{
public void map(LongWritable fileOffset, Text lineContents,
OutputCollector<FloatWritable, Text> output, Reporter reporter)
throws IOException {
String[] lineData = lineContents.toString().split("\\s+");
String prod="";
prod = lineContents.toString().replaceAll("\\d+.*", "");
//for(int i=0;i<lineData.length-1;i++)
//{
  //  prod=prod+lineData[i];
//}

Text ip = new Text(prod);
FloatWritable count = new FloatWritable(Float.parseFloat(lineData[lineData.length-1].trim()));
output.collect(count,ip);
}
}

public static class IpReducer1 extends MapReduceBase implements Reducer<FloatWritable, Text, Text, FloatWritable>

{
   private static int count1=0;
public void reduce(FloatWritable count, Iterator<Text> ip, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException
{
    
while(ip.hasNext())
{
    if(count1++ >=10)
        break;
output.collect(ip.next(),count);
}
}
}
public static class MyComparator extends WritableComparator {

		public MyComparator() {
			super(IntWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
			Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

			return (v1.compareTo(v2)) * -1;
		}
	}



  /**
   * @param args
   */
  public static void main(String[] args) throws Exception
  {
        JobConf conf = new JobConf(FinalPart3.class);
JobConf conf1 = new JobConf(FinalPart3.class);
        conf.setJobName("top10");
conf1.setJobName("top10");
        
        conf.setMapperClass(IpMapper.class);
conf1.setMapperClass(IpMapper1.class);
        
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(FloatWritable.class);
conf1.setMapOutputKeyClass(IntWritable.class);
conf1.setMapOutputValueClass(Text.class);
        
        conf.setReducerClass(IpReducer.class);
conf1.setReducerClass(IpReducer1.class);
 FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path("temp"));
      
        JobClient.runJob(conf);
      
      
        JobConf conf2 = new JobConf(FinalPart3.class);
        conf2.setJobName("ip-sort2");
        conf2.setMapperClass(IpMapper1.class);
        conf2.setMapOutputKeyClass(FloatWritable.class);
	      conf2.setMapOutputValueClass(Text.class);
        
        conf2.setOutputKeyComparatorClass(MyComparator.class);
        conf2.setNumReduceTasks(1);
        conf2.setReducerClass(IpReducer1.class);

        
        FileInputFormat.setInputPaths(conf2, new Path("temp"));
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));
        JobClient.runJob(conf2);
        

	}
    
}
