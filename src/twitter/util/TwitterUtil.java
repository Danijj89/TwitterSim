package twitter.util;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.mysql.cj.x.protobuf.MysqlxSql;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomStringUtils;
import twitter.database.MySQLDatabaseOP;
import twitter.database.MySQLDatabaseOPImpl;
import twitter.database.Tweet;

/**
 * Utility class that provides functionalities to build a tweet json file.
 */
public class TwitterUtil {

  /**
   * Builds a list of of tweets of a given size.
   * The userId of the tweet is randomly generated and the value
   * ranges from 1 to a given upper bound.
   * The datetime is generated randomly given a 'from' and a 'to' year (both inclusive).
   * The message are randomly generated.
   * The result is written to a given filePath.
   *
   * NOTE: Years must be >= 1000.
   *
   * @param numTweets the number of tweets to be generated.
   * @param numUsers the range of user ids that will appear in the resulting list (from 0).
   * @param fromYear the lower bound year for the datetime range.
   * @param toYear the upper bound year for the datetime range.
   * @param toFilePath the path of the file to save the result to.
   */
  public void buildTweets(long numTweets, int numUsers, int fromYear, int toYear,
      String toFilePath) {
    if (numTweets < 1) {
      throw new IllegalArgumentException("Given tweet number is negative or 0");
    }
    if (numUsers < 1) {
      throw new IllegalArgumentException("Given number of users is negative or 0");
    }
    if (fromYear < 1000 || toYear < 1000) {
      throw new IllegalArgumentException("Given years are negative");
    }
    if (fromYear > toYear) {
      throw new IllegalArgumentException("Given from year is bigger than to year");
    }
    if (toFilePath == null) {
      throw new IllegalArgumentException("Given messages or toFilePath is null");
    }
    JsonWriter writer = null;
    try {
      writer = new JsonWriter(new FileWriter(toFilePath));
      writer.setIndent("    ");
      writer.beginArray();
      Random userIdRandomizer = new Random();
      for (int i = 0; i < numTweets; i++) {
        int userId = userIdRandomizer.nextInt(numUsers) + 1;
        String datetime = this.generateDT(fromYear, toYear);
        String message = RandomStringUtils.randomAlphanumeric(userIdRandomizer.nextInt(140));
        Tweet t = new Tweet(i + 1, userId, datetime, message);
        this.writeMessage(writer, t);
      }
      writer.endArray();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void buildFollowTable(int fromUser, int toUser, int numFollowee, String filePath) {
    if (fromUser > toUser) {
      throw new IllegalArgumentException("The from user id is bigger than the to user id");
    }
    if (numFollowee < 1) {
      throw new IllegalArgumentException("The number of followee per person has to be positive");
    }
    JsonWriter writer = null;
    try {
      writer = new JsonWriter(new FileWriter(filePath));
      writer.setIndent("    ");
      writer.beginArray();
      Random userId = new Random();
      for (int i = fromUser; i <= toUser; i++) {
        int counter = numFollowee;
        while (counter > 0) {
          int follows = userId.nextInt(toUser - fromUser + 1) + fromUser;
          if (follows != i) {
            writer.beginObject();
            writer.name("user_id").value(i);
            writer.name("follows_id").value(follows);
            writer.endObject();
            counter--;
          }
        }
      }
      writer.endArray();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Computes the speed of writing Tweets to JSON file.
   *
   * @param filePath the path of the file to write to.
   * @param numOfTweets the number of tweets to write.
   * @param numUsers the number of unique users that will appear in the written file.
   * @return the number of tweets written per second.
   */
  public long getWriteSpeed(String filePath, long numOfTweets, int numUsers) {
    long start = System.currentTimeMillis();
    this.buildTweets(
        numOfTweets, numUsers, 2018, 2019, filePath);
    long end = System.currentTimeMillis();
    double totalTimeInSec = (end - start) / 1000;
    double tweetsXSec = numOfTweets / totalTimeInSec;
    return (long)tweetsXSec;
  }

  /**
   * Computes the speed of reading Tweets from a JSON file.
   *
   * @param filePath the path to the file to be read.
   * @param numOfHomeTM the number of home time-lines to be retrieved
   * @return
   */
  public long getReadSpeed(String filePath, long numOfHomeTM) {
    List<Tweet> tweets = new ArrayList<>();
    long start = 0;
    long end = 0;
    long counter = numOfHomeTM;
    try {
      start = System.currentTimeMillis();
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      while (reader.hasNext() && counter > 0) {
        tweets.add(this.readTweet(reader));
        counter--;
      }
      reader.endArray();
      reader.close();
      end = System.currentTimeMillis();
    } catch (IOException e) {
      e.printStackTrace();
    }
    double totalTime = (end - start) / 1000;
    double homeTMXSec = numOfHomeTM / totalTime;
    return (long)homeTMXSec;
  }

  /**
   * Helps read a single tweet and from JSON file.
   *
   * @param reader the {@link JsonReader} to read the tweet from.
   * @return the Tweet read.
   * @throws IOException if is it unable to read from the reader.
   */
  private Tweet readTweet(JsonReader reader) throws IOException {
    // might be able to remove the tweet_id condition
    List<Tweet> tweets = new ArrayList<>();
    long tweet_id = -1;
    int userId = -1;
    String datetime = null;
    String message = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("tweet_id")) {
        tweet_id = reader.nextLong();
      }
      else if (name.equals("user_id")) {
        userId = reader.nextInt();
      }
      else if (name.equals("datetime")) {
        datetime = reader.nextString();
      }
      else if (name.equals("message")) {
        message = reader.nextString();
      }
      else {
        reader.skipValue();
      }
    }
    reader.endObject();
    if (userId == -1 || datetime == null || message == null) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    return new Tweet(tweet_id, userId, datetime, message);
  }

  /**
   * Generates a random mysql DATETIME.
   *
   * @param fromYear the year lower bound.
   * @param toYear the year upper bound.
   * @return a string formatted as a mysql DATETIME.
   */
  public String generateDT(int fromYear, int toYear) {
    Random r = new Random();
    int year = r.nextInt(toYear - fromYear + 1) + fromYear;
    int month = r.nextInt(12) + 1;
    int day = r.nextInt(28) + 1;
    int hour = r.nextInt(24);
    int minute = r.nextInt(60);
    int second = r.nextInt(60);
    return String.format("%04d-%02d-%02d %02d:%02d:%02d",
        year, month, day, hour, minute, second);
  }

  /**
   * Writes a given tweet to a given {@link JsonWriter}.
   *
   * @param writer the {@link JsonWriter}.
   * @param tweet the tweet to be written.
   * @throws IOException if the given writer is unable to write.
   */
  private void writeMessage(JsonWriter writer, Tweet tweet) throws IOException {
    writer.beginObject();
    writer.name("tweet_id").value(tweet.getTweetId());
    writer.name("user_id").value(tweet.getUserId());
    writer.name("datetime").value(tweet.getDatetime());
    writer.name("message").value(tweet.getMessage());
    writer.endObject();
  }
}
