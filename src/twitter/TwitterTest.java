package twitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import twitter.database.MySQLDatabaseOP;
import twitter.database.MySQLDatabaseOPImpl;
import twitter.database.Tweet;
import twitter.util.TwitterUtil;

public class TwitterTest {

  public static void main(String[] args) {

    long numOfTweets = 1000000;
    int numOfUsers = 50000;
    int numFollowRelationXuser = 20;
    int limitHomeTM = 10;

    // Code used to produce initial files.

    TwitterUtil util = new TwitterUtil();
    util.buildTweets(numOfTweets, numOfUsers, 2015, 2018,"tweets.json");
    util.buildFollowTable(1, numOfUsers, numFollowRelationXuser, "follows.json");
    MySQLDatabaseOP op = new MySQLDatabaseOPImpl();

    op.connect("com.mysql.cj.jdbc.Driver",
        "jdbc:mysql://localhost:3306/twitter?user=root&password=perAdun0!");

    op.addFollowers("follows.json");

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

    // Start home timeline retrieval speed test
    Random r = new Random();
    List<Tweet> resultHomeTM = new ArrayList<>();
    long totalHomeTMSize = 0;
    start = System.currentTimeMillis();
    for (int i = 0; i < 50; i++) {
      int user_id = r.nextInt(numOfUsers) + 1;
      resultHomeTM = op.getHomeTM(user_id, limitHomeTM);
      totalHomeTMSize += resultHomeTM.size();
    }
    end = System.currentTimeMillis();
    long totalHomeTM = (end - start) / 1000;
    long homeTMXsec = 50 / totalHomeTM;
    long avgHomeTMsize = totalHomeTMSize / 50;
    System.out.format("Home timelines retrieved per second is %d\n", homeTMXsec);
    System.out.format("Home timelines average size retrieved is %d\n", avgHomeTMsize);

    op.closeConnection();
  }
}
