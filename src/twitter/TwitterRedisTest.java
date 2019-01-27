package twitter;

import java.util.Random;
import twitter.database.RedisDBOPImplStrategy1;
import twitter.database.RedisDBOPImplStrategy2;
import twitter.database.RedisDBOPImplStrategy3;
import twitter.database.RedisTwitterDatabaseOP;



/**
 * Main class for testing the performance of reads and writes using Redis.
 */
public class TwitterRedisTest {
  static int numUsers = 50000;
  static int numOfFollowers = 20;
  static long numTweets = 1000000;
  static int numOfTweetsInHomeTM = 10;
  static int numOfHomeTMToRetrieve = 1000;

  public static void main(String[] args) {
    // datetime format "yyyy-MM-dd HH:mm:ss"

    /*
    // Initial tweets and follow relations setup
    TwitterUtil util = new TwitterUtil();
    util.buildFollowTable(1, numUsers, numOfFollowers, "follows.json");
    util.buildTweets(numTweets, numUsers, "tweets.json");
    System.out.println("Done initializing files");
    */

    //testStrat1();

    //testStrat2();

    testStrat3();


  }

  static public void testStrat1() {
    RedisTwitterDatabaseOP strat1 = new RedisDBOPImplStrategy1("yyyy-MM-dd HH:mm:ss");

    //strat1.resetDatabase();

    //strat1.addFollowers("follows.json");

    /*
    // write performance test
    long start = System.currentTimeMillis();
    strat1.addTweets("tweets.json");
    long end = System.currentTimeMillis();

    double totalTime = (end - start) / 1000.0;
    long avgWritesXsec = (long)(numTweets / totalTime);
    System.out.format("Average writes per second = %d\n",avgWritesXsec);
    */


    // read perfomance test
    long start2 = System.currentTimeMillis();
    Random r = new Random();
    for (int i = 0; i < numOfHomeTMToRetrieve; i++) {
      String id = String.valueOf(r.nextInt(numUsers) + 1);
      strat1.getHomeTM(id, numOfTweetsInHomeTM);
    }
    long end2 = System.currentTimeMillis();

    double totalTime2 = (end2 - start2) / 1000.0;
    long avgReadXsec = (long)(numOfHomeTMToRetrieve / totalTime2);
    System.out.format("Average reads per second = %d\n", avgReadXsec);
    strat1.closeConnection();
  }

  static public void testStrat2() {
    RedisTwitterDatabaseOP strat2 = new RedisDBOPImplStrategy2("yyyy-MM-dd HH:mm:ss");

    strat2.resetDatabase();

    strat2.addFollowers("follows.json");

    // write performance test
    long start = System.currentTimeMillis();
    strat2.addTweets("tweets.json", true);
    long end = System.currentTimeMillis();

    double totalTime = (end - start) / 1000.0;
    long avgWritesXsec = (long)(numTweets / totalTime);
    System.out.format("Average writes per second = %d\n",avgWritesXsec);

    // read perfomance test
    long start2 = System.currentTimeMillis();
    Random r = new Random();
    for (int i = 0; i < numOfHomeTMToRetrieve; i++) {
      String id = String.valueOf(r.nextInt(numUsers) + 1);
      strat2.getHomeTM(id, numOfTweetsInHomeTM);
    }
    long end2 = System.currentTimeMillis();

    double totalTime2 = (end2 - start2) / 1000.0;
    long avgReadXsec = (long)(numOfHomeTMToRetrieve / totalTime2);
    System.out.format("Average reads per second = %d\n", avgReadXsec);
    strat2.closeConnection();
  }

  static public void testStrat3() {
    RedisTwitterDatabaseOP strat3 = new RedisDBOPImplStrategy3("yyyy-MM-dd HH:mm:ss");

    //strat3.resetDatabase();

    //strat3.addFollowers("follows.json");

    /*
    // write performance test
    long start = System.currentTimeMillis();
    strat3.addTweets("tweets.json", true);
    long end = System.currentTimeMillis();

    double totalTime = (end - start) / 1000.0;
    long avgWritesXsec = (long)(numTweets / totalTime);
    System.out.format("Average writes per second = %d\n",avgWritesXsec);
    */

    // read perfomance test
    long start2 = System.currentTimeMillis();
    Random r = new Random();
    for (int i = 0; i < numOfHomeTMToRetrieve; i++) {
      String id = String.valueOf(r.nextInt(numUsers) + 1);
      strat3.getHomeTM(id, numOfTweetsInHomeTM);
    }
    long end2 = System.currentTimeMillis();

    double totalTime2 = (end2 - start2) / 1000.0;
    long avgReadXsec = (long)(numOfHomeTMToRetrieve / totalTime2);
    System.out.format("Average reads per second = %d\n", avgReadXsec);
    strat3.closeConnection();
  }
}
