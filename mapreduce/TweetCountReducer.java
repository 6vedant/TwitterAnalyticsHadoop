import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
 
public class TweetCountReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		

		String userName = key.toString();
		System.out.println("User Name received : " + userName);

		int totalRetweetCount = 0;
		int totalFavouriteCount = 0;
		int netTweetCount = 0;
		int individualCounts = 0;

		for (Text retweetNFavouriteToken : values) {

			System.out.println("Token value received : "
					+ retweetNFavouriteToken.toString());
			String[] retweetNFavouriteTokenArray = retweetNFavouriteToken
					.toString().split("@~");

			String retweetCountString = retweetNFavouriteTokenArray[0];
			String favouriteCountString = retweetNFavouriteTokenArray[1];

			int retweetCount = isNumeric(retweetCountString) ? Integer
					.valueOf(retweetCountString) : 0;
			int favouriteCount = isNumeric(favouriteCountString) ? Integer
					.valueOf(favouriteCountString) : 0;

			totalRetweetCount += retweetCount;
			totalFavouriteCount += favouriteCount;
			individualCounts++;

		}

		// Calculating net tweet count value of the tweet.
		netTweetCount = totalRetweetCount * 2 + totalFavouriteCount * 5 + individualCounts;

		String tokenisedValues = "^".concat(String.valueOf(netTweetCount));
		context.write(key, new Text(tokenisedValues));
		
		
	}
	 
	public static boolean isNumeric(String str) {
		try {
			Integer i = Integer.valueOf(str);
		} catch (NumberFormatException nfe) {
			return false;
		}
		return true;
	}
}