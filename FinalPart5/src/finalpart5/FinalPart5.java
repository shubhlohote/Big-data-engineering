/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package finalpart5;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author shubhangi
 */
public class FinalPart5 extends Configured implements Tool {
	public static class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {
		Funnel<Product> p = new Funnel<Product>() {

			public void funnel(Product product, Sink into) {
				// TODO Auto-generated method stub
				into.putString(product.name, Charsets.UTF_8)
						.putString(product.issue, Charsets.UTF_8);
			}

		};
		private BloomFilter<Product> friends = BloomFilter.create(p, 4, 0.1);

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			Product p1 = new Product( "Bank account or service", "Deposits and withdrawals");
			
                       
			ArrayList<Product> friendsList = new ArrayList<Product>();
			friendsList.add(p1);
			

			for (Product pr : friendsList) {
				friends.put(pr);
			}
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String values[] = value.toString().split(",");
			Product p = new Product(values[3], values[5]);
			if (friends.mightContain(p)) {
				context.write(value, NullWritable.get());
			}

		}

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new FinalPart5(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Bloom Filter");
		job.setJarByClass(FinalPart5.class);
		job.setMapperClass(BloomFilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(
				args[0]));
		FileOutputFormat.setOutputPath(job,
				new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		System.out.println(success);
		return success ? 0 : 1;
	}
}
