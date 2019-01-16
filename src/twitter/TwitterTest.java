package twitter;

import twitter.util.TwitterUtil;

public class TwitterTest {

  public static void main(String[] args) {
    TwitterUtil util = new TwitterUtil();
    util.buildTweets(20000, 5000, 1990, 1991,"tweets.json");
    util.buildFollowTable(1, 5000, 20, "follows.json");
  }
}
