package twitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import twitter.database.MySQLDatabaseOP;
import twitter.database.MySQLDatabaseOPImpl;
import twitter.database.Tweet;
import twitter.util.TwitterUtil;

public class TwitterMySqlTest {

  public static void main(String[] args) {
    long numOfTweets = 1000000;
    int numOfUsers = 20000;
    int numFollowRelationXuser = 20;
    int limitHomeTM = 10;
    String password = null;

    // Code used to produce initial files.

    TwitterUtil util = new TwitterUtil();

    util.buildTweets(numOfTweets, numOfUsers,"tweets.json");
    util.buildFollowTable(1, numOfUsers, numFollowRelationXuser, "follows.json");
    MySQLDatabaseOP op = new MySQLDatabaseOPImpl();

    op.connect("com.mysql.cj.jdbc.Driver",
        "jdbc:mysql://localhost:3306/twitter?user=root&password=" + password);

    op.addFollowers("follows.json");

    // Test performance of reading and writing to disk

    long writeToFileTime = util.getWriteSpeed("tweets.json", numOfTweets, numOfUsers);
    System.out.format("Rate of tweets written to file per second = %d\n", writeToFileTime);

    long readFromFileTime = util.getReadSpeed("tweets.json", limitHomeTM);
    System.out.format("Rate of tweets read from file per second = %d\n",readFromFileTime);

    // Start tweet writing speed test


    long start = System.currentTimeMillis();
    op.addTweets("tweets.json");
    long end = System.currentTimeMillis();
    double totalTweetTime = (end - start) / 1000.0;
    double tweetXsec = numOfTweets / totalTweetTime;
    System.out.println(
        String.format("Tweets written per second is %d", (long)tweetXsec));


    // Start home timeline retrieval speed test
    Random r = new Random();
    List<Tweet> resultHomeTM = new ArrayList<>();
    long totalHomeTMSize = 0;
    start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      String user_id = String.valueOf(r.nextInt(numOfUsers) + 1);
      resultHomeTM = op.getHomeTM(user_id, limitHomeTM);
      totalHomeTMSize += resultHomeTM.size();
    }
    end = System.currentTimeMillis();
    System.out.println(start);
    System.out.println(end);
    double totalHomeTM = (end - start) / 1000.0;
    double homeTMXsec = 100 / totalHomeTM;
    long avgHomeTMsize = totalHomeTMSize / 100;
    System.out.format("Home timelines retrieved per second is %d\n", (long)homeTMXsec);
    System.out.format("Home timelines average size retrieved is %d\n", avgHomeTMsize);


    op.closeConnection();
  }
}