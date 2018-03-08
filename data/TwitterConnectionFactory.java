
/*crated by jhavedant*/

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

public class TwitterConnectionFactory {

	public static Twitter getTwitterConnection() {

		Twitter twitter = new TwitterFactory().getInstance();
		twitter.setOAuthConsumer("yourConsumerKeyGeneratedFromTwitterApp",
				"yourConsumerSecretGeneratedFromTwitterApp");
		twitter.setOAuthAccessToken(new AccessToken(
				"yourTokenGeneratedFromTwitterApp",
				"yourTokenSecretGeneratedFromTwitterApp"));
		
		return twitter;
	}
}
