/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart12;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author shubhangi
 */
public class FinalPart12 {

    /**
     * @param args the command line arguments
     */
    public static class MedianMapper extends Mapper<Object, Text,Text,SortedMapWritable> {
    private Text state = new Text();
    private Text company = new Text();
    private static final LongWritable one=new LongWritable(1);
    
    
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                System.out.println(se[1]);
                System.out.println(se[2]);
                state = new Text(se[2]);
                company=new Text(se[1]);
                

            }
        SortedMapWritable outRating=new SortedMapWritable();
        outRating.put(company, one);
        
        
        context.write(state, outRating);
        
    }
    
    
}
    public static class MvCombiner extends
                Reducer<Text, SortedMapWritable, Text, SortedMapWritable> {

            @SuppressWarnings("rawtypes")
            protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context)
                    throws IOException, InterruptedException {

                SortedMapWritable outValue = new SortedMapWritable();

                for (SortedMapWritable v : values) {
                    for (Map.Entry<WritableComparable, Writable> entry : v.entrySet()) {
                        FloatWritable count = (FloatWritable) outValue.get(entry
                                .getKey());
                        if (count != null) {
                            count.set(count.get()
                                    + ((FloatWritable) entry.getValue()).get());
                        } else {
                            outValue.put(entry.getKey(), new FloatWritable(
                                    ((FloatWritable) entry.getValue()).get()));
                        }
                    }
                }

                context.write(key, outValue);
            }
        }
    
    public static class MedianReducer extends Reducer<Text,SortedMapWritable,Text,CacheTuple>{
    private CacheTuple result=new CacheTuple();
    private TreeMap<Text,Long> rating=new TreeMap<Text, Long>();
    public void reduce(Text key,Iterable<SortedMapWritable> values,Context context) throws IOException, InterruptedException{
        
        long totalRating=0;
        rating.clear();
        
        result.setCount(0);
        for(SortedMapWritable v:values)
        {
            for(Map.Entry<WritableComparable,Writable> entry:v.entrySet())
            {
                Text comp=(Text)entry.getKey();
                long count=((LongWritable)entry.getKey()).get();
                totalRating+=count;
                
                Long storedCount=rating.get(comp);
                if(storedCount== 0)
                {
                  rating.put(comp, count);
                }
                else
                {
                    rating.put(comp, storedCount+count);
                }
                
                
               
            }
        }
        //result.setCompany(rating.get(key));
        //result.setCompany(rating.get(key));
        
        context.write(key, result);
        
    }
    
    
    
}


    
}
