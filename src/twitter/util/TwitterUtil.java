package twitter.util;

import com.google.gson.stream.JsonWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
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
   * The message comes from an implementation specific iterator.
   * The result is written to a given filePath.
   *
   * NOTE: Years must be >= 1000.
   *
   * @param numTweets the number of tweets to be generated.
   * @param numUsers the range of user ids that will appear in the resulting list (from 0).
   * @param fromYear the lower bound year for the datetime range.
   * @param toYear the upper bound year for the datetime range.
   * @param messages the {@link Iterator} from which to retrieve the messages from.
   * @param toFilePath the path of the file to save the result to.
   */
  public void buildTweets(long numTweets, int numUsers, int fromYear, int toYear,
      Iterator<String> messages, String toFilePath) {
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
    if (messages == null || toFilePath == null) {
      throw new IllegalArgumentException("Given messages or toFilePath is null");
    }
    JsonWriter writer = null;
    try {
      writer = new JsonWriter(new FileWriter(toFilePath));
      writer.setIndent("    ");
      writer.beginArray();
      Random userIdRandomizer = new Random();
      for (int i = 0; i < numTweets; i++) {
        if (!messages.hasNext()) {
          throw new IllegalStateException("Given messages are not enough!");
        }
        else {
          int userId = userIdRandomizer.nextInt(numUsers) + 1;
          String datetime = this.generateDT(fromYear, toYear);
          String message = messages.next();
          Tweet t = new Tweet(i + 1, userId, datetime, message);
          this.writeMessage(writer, t);
        }
      }
      writer.endArray();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
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
