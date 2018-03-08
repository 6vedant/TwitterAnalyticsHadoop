import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.URLEntity;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;

/**
 * @author jhavedant
 *
 */

public class FetchTwitterData {

	/**
	 * @param args
	 * @throws TwitterException
	 */
	public static void main(String[] args) throws TwitterException {

		String trendingKeyword = "hadoop";

		Twitter twitter = TwitterConnectionFactory.getTwitterConnection();
		ArrayList<String> tweetList = new ArrayList<String>();

		// The tweets will be written to E:\\HDP\\HPT\\project\\trendingTopicTweeters.txt file . Can be changed in case of requirement.
		
		String fileName = "trendingTopicTweeters";
		String basePath = "E:\\HDP\\HPT\\project\\";
		File file = new File(basePath.concat(fileName).concat(".txt"));

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(new Date());
		calendar.add(Calendar.DATE, -2);
		Date filteringDate = calendar.getTime();

		String filteringDateString = new SimpleDateFormat("yyyyMMdd")
				.format(filteringDate);

		try {
			FileWriter fw = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(fw);

			if (file.exists()) {
				long currentTime = Calendar.getInstance().getTimeInMillis();
				String newFileName = basePath.concat(fileName).concat("_")
						.concat(String.valueOf(currentTime));
				file.renameTo(new File(newFileName));
			} else {
				file.createNewFile();

			}

			// Fetching tweets for last 2 days or max 100 tweets whichever is
			// minimum

			Query query = new Query(trendingKeyword);
			query.setSince(filteringDateString);
			QueryResult result;

			int counter = 0;
			do {
				result = twitter.search(query);
				List<Status> tweets = result.getTweets();
				
				System.out.println("Fetching tweets occuring in a page");

				boolean hasInnerLoopBreaked = false;
				for (Status tweet : tweets) {
					if (counter == 3000) {
						hasInnerLoopBreaked = true;
					} else {
						
						counter = processTweets(trendingKeyword, tweetList, bw,
								counter, tweet);
					}
				}

				if (hasInnerLoopBreaked) {
					break;
				}
			} while ((query = result.nextQuery()) != null);

			bw.close();

			System.out.println("Data Fetching completed succesfully..");
		}

		catch (IOException e) {
			e.printStackTrace();
		} catch (TwitterException te) {
			te.printStackTrace();
			System.out.println("Failed to search tweets: " + te.getMessage());
		}

	}

	private static int processTweets(String trendingKeyword,
			ArrayList<String> tweetList, BufferedWriter bw, int counter,
			Status tweet) throws IOException {
		
		String tweetText = tweet.isRetweeted() ? tweet.getRetweetedStatus().getText() : tweet.getText();	
		int retweetCount = tweet.getRetweetCount();		
		
		counter = basicFilterAndWritingToFile(trendingKeyword,
				tweetList, bw, counter, tweet, tweetText,
				retweetCount);
		return counter;
	}

	private static int basicFilterAndWritingToFile(String trendingKeyword,
			ArrayList<String> tweetList, BufferedWriter bw, int counter,
			Status tweet, String tweetText, int retweetCount)
			throws IOException {
		if (tweetText.toLowerCase().contains(trendingKeyword)) {

			counter += 1;
			int favouriteCount = tweet.getFavoriteCount();

			tweetList.add(tweet.getText());

			String tweetSource = tweet.getUser().getName();
			
			writeToStream(bw, retweetCount, favouriteCount,
					tweetSource);
			
			// Limiting tweet value i.e 100 here can be changed as per requirement.
			// Here only 100 tweets are fetched.
			if (counter != 3000) {
				// Avoids new line at the end of file.
				bw.newLine();
			}

		}
		return counter;
	}

	private static void writeToStream(BufferedWriter bw, int retweetCount,
			int favouriteCount, String tweetSource) throws IOException {
		bw.write(UUID.randomUUID().toString());
		bw.write("~9");
		bw.write(UUID.randomUUID().toString());
		bw.write("~9");
		bw.write(tweetSource);
		bw.write("~9");
		bw.write(String.valueOf(retweetCount));
		bw.write("~9");
		bw.write(UUID.randomUUID().toString());
		bw.write("~9");
		bw.write(UUID.randomUUID().toString());
		bw.write("~9");
		bw.write(UUID.randomUUID().toString());
		bw.write("~9");
		bw.write(favouriteCount);
		bw.write("~9");
		bw.write(UUID.randomUUID().toString());
		bw.write("~9");
		bw.write(UUID.randomUUID().toString());
	}

}
