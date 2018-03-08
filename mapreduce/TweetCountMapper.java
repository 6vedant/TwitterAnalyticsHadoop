import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class TweetCountMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	
	
	private static List<String> toBeUnconsideredUsers = null;
	
	public void map(LongWritable mapperInputKey, Text rawTweetStream,
			Context context) throws IOException, InterruptedException {
		
		String tweetStreamPerLine = rawTweetStream.toString();
		

		System.out.println("Tweet Line Received is : "
				+ tweetStreamPerLine);		

		// Extraction of user name and retweet count and favourites count
		String[] values = tweetStreamPerLine.split("~9");

		String tweetUserName = values[2];
		String retweetCount = values[3];
		String favouriteCount = values[7];
		
		String retweetNFavouriteCount = retweetCount.concat("@~").concat(
				favouriteCount);
		
		if(CollectionUtils.isEmpty(toBeUnconsideredUsers)) {
			toBeUnconsideredUsers = ListOfUnConsideredUsers.getUnConsideredUsersList();
		}
		
		if(tweetUserName != null && !tweetUserName.equalsIgnoreCase("")) {
			
			String[] userNameWords = tweetUserName.split(" ");
			if(userNameWords[0].equalsIgnoreCase("RT")) {
				tweetUserName = tweetUserName.substring(3);				
			}
			tweetUserName = tweetUserName.trim();
			tweetUserName = tweetUserName.replace("^", "");
			System.out.println("Extracted user name : " + tweetUserName);
			
			boolean isUserEligibleToBeConsidered = toBeUnconsideredUsers.contains(tweetUserName) ? false : true;
			
			System.out.println("User Eligibility : " + isUserEligibleToBeConsidered);
			System.out.println("Retweet And Favourite Count Token :" + retweetNFavouriteCount);
			
			if(isUserEligibleToBeConsidered) {
				context.write(new Text(tweetUserName), new Text(retweetNFavouriteCount));
			}
		}
	}
}