package twitter.util;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Utility class that provides functionalities to build a tweet json file.
 */
public class TwitterUtil {
  private final Readable rd;
  private final Appendable ap;

  TwitterUtil

  /**
   * Builds a list of of tweets of a given size.
   * The userId of the tweet is randomly generated and the value
   * ranges from 1 to a given upper bound.
   * The datetime is generated randomly given a 'from' and a 'to' year (both inclusive).
   * The message comes from parsing a given file and turned into an iterator.
   * The result is saved into a given {@link Appendable}.
   *
   * @param numTweets the number of tweets to be generated.
   * @param numUsers the range of user ids that will appear in the resulting list (from 0).
   * @param fromYear the lower bound year for the datetime range.
   * @param toYear the upper bound year for the datetime range.
   * @param parser the parser to parse the given file path.
   * @param fromFile the path to the file from which to read the messages.
   * @param ap the appendable to append the result to.
   */
  public void buildTweets(long numTweets, int numUsers, int fromYear, int toYear,
      TwitterParser parser, String fromFile, Appendable ap) throws Exception {
    JsonReader reader = null;
    JsonWriter writer = null;
    try {
      writer = new JsonWriter(new FileWriter("tweets.json"));
      writer.setIndent("    ");
      reader = new JsonReader(new FileReader("reddit_jokes.json"));
      writer.beginArray();
      reader.beginArray();
      Random userIdRandomizer = new Random();
      for (int i = 0; i < numTweets; i++) {
        int userId = userIdRandomizer.nextInt(50000) + 1;
        String datetime =
      }
    } catch (IOException e) {

    } finally {

    }
    if (reader != null) {

    }
    reader.close();
    writer.close();
  }

  public String generateDT(int fromYear, int toYear) {

  }

}
