package twitter;

import twitter.database.RedisDatabaseOPImpl;
import twitter.util.TwitterUtil;


/**
 * Main class for testing the performance of reads and writes using Redis.
 */
public class TwitterRedisTest {

  public static void main(String[] args) {
    int numUsers = 50000;
    int numOfFollowers = 20;
    int numTweets = 1000000;
    // datetime format "yyyy-MM-dd HH:mm:ss"

    // Initial tweets and follow relations setup
    TwitterUtil util = new TwitterUtil();
    util.buildFollowTable(1, numUsers, numOfFollowers, "follows.json");
    util.buildTweets(numTweets, numUsers, "tweets.json");

    // Perfomance Tests
    RedisDatabaseOPImpl db = new RedisDatabaseOPImpl("yyyy-MM-dd HH:mm:ss");

    // Flush DB if necessary
    db.resetDatabase();

    // Strategy 1


  }

}
