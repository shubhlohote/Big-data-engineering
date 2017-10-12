/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart13;

/**
 *
 * @author shubhangi
 */
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class FinalPart13 {

    /**
     * @param args the command line arguments
     */
    public static class WordcountMapper extends
		Mapper<Object, Text, Text, Text> {

	private Text issue = new Text();
	private Text id = new Text();

	private boolean caseSensitive = false;

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		//filename = new Text(filenameStr);
		
		String line = value.toString();

		if (!caseSensitive) {
			line = line.toLowerCase();
		}

		//String line = value.toString();
            String se[] = line.split(",");
            if (se[0].equals("Date received") ) {
            } else {
                //System.out.println(se[1]);
               // System.out.println(se[2]);
                issue = new Text(se[5]);
                id = new Text(se[17]);
                context.write(issue, id);

            }
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.caseSensitive = conf.getBoolean("wordcount.case.sensitive",false);
	}
}
    public static class WordcountReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {

		StringBuilder stringBuilder = new StringBuilder();

		for (Text value : values) {
			stringBuilder.append(value.toString());

			if (values.iterator().hasNext()) {
				stringBuilder.append(" -> ");
			}
		}

		context.write(key, new Text(stringBuilder.toString()));
	}

}
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "StockVolume Min Max date");
        job.setJarByClass(FinalPart13.class);
        job.setMapperClass(WordcountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(WordcountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
