package twitter;

import java.util.Random;
import twitter.database.MySQLDatabaseOP;
import twitter.database.MySQLDatabaseOPImpl;
import twitter.util.TwitterUtil;

public class TwitterTest {

  public static void main(String[] args) {
    long numOfTweets = 50000;
    // Code used to produce initial files.

    int numOfUsers = 5000;
    int numFollowRelationXuser = 20;
    TwitterUtil util = new TwitterUtil();
    util.buildTweets(numOfTweets, numOfUsers, 2015, 2018,"tweets.json");
    util.buildFollowTable(1, numOfUsers, numFollowRelationXuser, "follows.json");
    MySQLDatabaseOP op = new MySQLDatabaseOPImpl();
    op.connect("com.mysql.cj.jdbc.Driver",
        "jdbc:mysql://localhost:3306/twitter?user=root&password=perAdun0!");

    // Start tweet writing speed test

    long start = System.currentTimeMillis();
    op.addTweets("tweets.json");
    long end = System.currentTimeMillis();
    long totalTweetTime = (end - start) / 1000;
    long tweetXsec = numOfTweets / totalTweetTime;
    System.out.println(
        String.format("Total time required to add %d tweets is %ds", numOfTweets, totalTweetTime));
    System.out.println(
        String.format("Tweets written per second is %d", tweetXsec));

    // Start followers writing speed test

    start = System.currentTimeMillis();
    op.addFollowers("follows.json");
    end = System.currentTimeMillis();
    long totalFollowerTime = (end - start) / 1000;
    long followersXsec = (numOfUsers * numFollowRelationXuser) / totalFollowerTime;
    System.out.println(
        String.format("Followers added per second is %d", followersXsec));

    // Start home timeline retrieval speed test
    Random r = new Random();
    start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      op.getHomeTM(r.nextInt(numOfUsers) + 1);
      System.out.println(i);
    }
    end = System.currentTimeMillis();
    long totalHomeTM = (end - start) / 1000;
    long homeTMXsec = 100 / totalHomeTM;
    System.out.println(
        String.format("Home timelines retrieved per second is %d", homeTMXsec));
    op.closeConnection();
  }
}
