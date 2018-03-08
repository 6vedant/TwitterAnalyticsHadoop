import java.io.IOException;
import java.util.Map.Entry;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author jhavedant
 *
 */
public class TweetCountDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out
				.println("************************** In main method ****************");

		Configuration configuration = new Configuration();
		TweetCountDriver dateTR = new TweetCountDriver();

		int exitCode = ToolRunner.run(configuration, dateTR, args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {

		Configuration conf = getConf();
		if (arg0.length != 2) {
			System.err.println("Usage: tweet analyser <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,
				"Sentimental Analytics of Tweets "
						+ Calendar.getInstance().getTime());
		
		job.setJarByClass(TweetCountDriver.class);
		job.setMapperClass(TweetCountMapper.class);
		job.setReducerClass(TweetCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(arg0[1]);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
