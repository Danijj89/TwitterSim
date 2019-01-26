package twitter.database;

import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
  public void addTweet(int userId, Calendar datetime, String message)
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
    long datetime = -1;
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
    if (userId == -1 || datetime == -1 || message == null) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    Calendar c = Calendar.getInstance();
    c.setTime(new Date(datetime));
    Tweet t = new Tweet(tweet_id, userId, c, message);
    this.addTweet(t);
  }

  @Override
  public void addTweet(Tweet t, boolean broadcast) {
    this.addTweet(t);
    if (broadcast) {
      int userId = t.getUserId();
      Set<String> followers = this.jedis.smembers("followers:" + String.valueOf(userId));
      for (String s : followers)
    }
  }

  @Override
  public void addFollower(int followerId, int followeeId) {
    String key = "followers:" + String.valueOf(followeeId);
    String value = String.valueOf(followerId);
    this.jedis.sadd(key,value);
  }

  @Override
  public void addFollowers(String filePath) {
    try {
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      while (reader.hasNext()) {
        this.addFollowerHelp(reader);
      }
      reader.endArray();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Parses a follower-followee relation from a {@link JsonReader} and inserts it into the database.
   *
   * @param reader the reader to read the json from.
   */
  private void addFollowerHelp(JsonReader reader) throws IOException {
    int follower_id = -1;
    int followee_id = -1;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("user_id")) {
        follower_id = reader.nextInt();
      }
      else if (name.equals("follows_id")) {
        followee_id = reader.nextInt();
      }
      else {
        reader.skipValue();
      }
    }
    reader.endObject();
    if (follower_id == -1 || followee_id == -1) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    this.addFollower(follower_id, followee_id);
  }

  @Override
  public List<Tweet> getHomeTM(int userId) {
    String key = "followers:" + String.valueOf(userId);

  }

  @Override
  public List<Tweet> getHomeTM(int userId, int numOfTweets) {
    return null;
  }

  @Override
  public List<Integer> getFollowers(int userId) {
    Set<String> followers = this.jedis.smembers("followers:" + String.valueOf(userId));
    List<Integer> result = followers.stream().map(s -> Integer.valueOf(s))
        .collect(Collectors.toList());
    return result;
  }

  @Override
  public void resetDatabase() {
    this.jedis.flushAll();
  }
}
