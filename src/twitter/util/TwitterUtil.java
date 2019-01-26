package twitter.util;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.mysql.cj.x.protobuf.MysqlxSql;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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
   * The datetime is generated using the {@code generateDT} function.
   * The message are randomly generated.
   * The result is written to a given filePath.
   *
   * NOTE: Years must be >= 1000.
   *
   * @param numTweets the number of tweets to be generated.
   * @param numUsers the range of user ids that will appear in the resulting list (from 0).
   * @param toFilePath the path of the file to save the result to.
   */
  public void buildTweets(long numTweets, int numUsers, String toFilePath) {
    if (numTweets < 1) {
      throw new IllegalArgumentException("Given tweet number is negative or 0");
    }
    if (numUsers < 1) {
      throw new IllegalArgumentException("Given number of users is negative or 0");
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
        String tweetId = String.valueOf(i + 1);
        String userId = String.valueOf(userIdRandomizer.nextInt(numUsers) + 1);
        Calendar datetime = this.generateDT();
        String message = RandomStringUtils.randomAlphanumeric(userIdRandomizer.nextInt(140));
        Tweet t = new Tweet(tweetId, userId, datetime, message);
        this.writeMessage(writer, t);
      }
      writer.endArray();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Builds a randomly generated list of pairs on the form of (follower, followed) and saves it
   * as JSON to a given file path.
   *
   * @param fromUser the lower bound from which user id starts.
   * @param toUser the upper bound to which user id go.
   * @param numFollowee the number of followee each user follow.
   * @param filePath the file path to which to save the list.
   */
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
        numOfTweets, numUsers, filePath);
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
    String tweet_id = null;
    String userId = null;
    long datetime = -1;
    String message = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("tweet_id")) {
        tweet_id = reader.nextString();
      }
      else if (name.equals("user_id")) {
        userId = reader.nextString();
      }
      else if (name.equals("datetime")) {
        datetime = reader.nextLong();
      }
      else if (name.equals("message")) {
        message = reader.nextString();
      }
      else {
        reader.skipValue();
      }
    }
    reader.endObject();
    if (userId == null || message == null || datetime == -1 || tweet_id == null) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    Calendar date = Calendar.getInstance();
    date.setTime(new Date(datetime));
    return new Tweet(tweet_id, userId, date, message);
  }

  /**
   * Generates a random {@link Calendar} using a random long value.
   *
   * @return the Date.
   */
  private Calendar generateDT() {
    Random r = new Random();
    Calendar c = Calendar.getInstance();
    c.setTime(new Date(r.nextLong()));
    return c;
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
    long date = tweet.getDatetime().getTime().getTime();
    writer.name("datetime").value(date);
    writer.name("message").value(tweet.getMessage());
    writer.endObject();
  }

}
