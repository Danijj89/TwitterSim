package twitter.database;

import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import redis.clients.jedis.Jedis;

/**
 * Represents an implementation of the Redis database operations for the Twitter project.
 */
public class RedisDatabaseOPImpl implements RedisTwitterDatabaseOP {

  // Establishes a new connection to the local default Redis DB.
  Jedis jedis = new Jedis();
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Override
  public void addTweet(Tweet t) {
    String key = "tweet:" + t.getTweetId();
    String datetime = sdf.format(t.getDatetime().getTime());
    String value = t.getUserId() + ":" + datetime + ":" + t.getMessage();
    this.jedis.set(key, value);
  }

  @Override
  public void addTweet(String userId, Calendar datetime, String message)
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
    if (userId == null || datetime == -1 || message == null || tweet_id == null) {
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
  public void addFollower(String followerId, String followeeId) {
    String key = "followers:" + followeeId;
    String value = followerId;
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
    String follower_id = null;
    String followee_id = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("user_id")) {
        follower_id = reader.nextString();
      }
      else if (name.equals("follows_id")) {
        followee_id = reader.nextString();
      }
      else {
        reader.skipValue();
      }
    }
    reader.endObject();
    if (follower_id == null || followee_id == null) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    this.addFollower(follower_id, followee_id);
  }

  @Override
  public List<Tweet> getHomeTM(String userId) {
    Set<String> homeTM = this.jedis.zrevrange("hometl:" + userId, 0, 10);
    Function<String, Tweet> f = new Function<String, Tweet>() {
      @Override
      public Tweet apply(String s) {
        try {
          String[] data = s.split(":");
          String userId = data[0];
          Calendar datetime = Calendar.getInstance();
          datetime.setTime(sdf.parse(data[1]));
          String message = data[2];
        } catch (ParseException e) {
          e.printStackTrace();
        }
        Tweet t = new Tweet(, , , )
      }
    }
    List<Tweet> result = homeTM.stream().map(s -> s.split(":"))

  }

  @Override
  public List<Tweet> getHomeTM(String userId, int numOfTweets) {
    return null;
  }

  @Override
  public Set<String> getFollowers(String userId) {
    Set<String> followers = this.jedis.smembers("followers:" + userId);
    return followers;
  }

  @Override
  public void resetDatabase() {
    this.jedis.flushAll();
  }
}
