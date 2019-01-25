package twitter.database;

import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import redis.clients.jedis.Jedis;

/**
 * Represents an implementation of the Redis database operations for the Twitter project.
 */
public class RedisDatabaseOPImpl implements RedisTwitterDatabaseOP {

  // Establishes a new connection to the local default Redis DB.
  Jedis jedis = new Jedis();

  @Override
  public void addTweet(Tweet t) {
    String key = new StringBuilder("tweet:").append(t.getUserId())
        .append(":").append(t.getTweetId()).toString();
    String value = t.getMessage();
    this.jedis.set(key, value);
  }

  @Override
  public void addTweet(int userId, String datetime, String message)
      throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTweets(String filePath) {
    try {
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      while (reader.hasNext()) {
        this.addTweetHelp(reader);
      }
      reader.endArray();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Parses a Tweet from a {@link JsonReader} and inserts it into the database.
   *
   * @param reader the reader to read the json  from.
   */
  private void addTweetHelp(JsonReader reader) throws IOException {
    // might be able to remove the tweet_id condition
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
    Tweet t = new Tweet(tweet_id, userId, datetime, message);
    this.addTweet(t);
  }

  @Override
  public void addTweet(Tweet t, boolean broadcast) {

  }

  @Override
  public void addFollower(int followerId, int followeeId) {

  }

  @Override
  public void addFollowers(String filePath) {

  }

  @Override
  public List<Tweet> getHomeTM(int userId) {
    return null;
  }

  @Override
  public List<Tweet> getHomeTM(int userId, int numOfTweets) {
    return null;
  }

  @Override
  public List<Integer> getFollowers(int userId) {
    return null;
  }

  @Override
  public void resetDatabase() {
    this.jedis.flushAll();
  }


}