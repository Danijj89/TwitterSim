package twitter.database;

import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import redis.clients.jedis.Jedis;

/**
 * Represents an implementation of the Redis database operations for the Twitter project.
 * Naming conventions used in this implementation:
 * 'nextTweetId' is a counter that keeps track of the next usable id to use as tweet id;
 * 'followers:(user_id)' is the format of keys for the set of followers of each user;
 * 'hometl:(user_id)' is the format of keys for the sorted list of tweets in the user home timeline;
 */
public class RedisDatabaseOPImpl implements RedisTwitterDatabaseOP {

  private final Jedis jedis;
  private final SimpleDateFormat sdf;

  /**
   * Establishes a new connection to the local default Redis DB upon construction.
   * Sets up the {@code nextTweetId} index to start at 1.
   * Initializes the format of datetime stored in the database.
   */
  public RedisDatabaseOPImpl(String datetimeFormat) {
    if (datetimeFormat == null) {
      throw new IllegalArgumentException("Given argument is null");
    }
    this.jedis = new Jedis();
    this.sdf = new SimpleDateFormat(datetimeFormat);
  }

  /**
   * Gets the next usable id to use as tweet id and increments the counter.
   *
   * @return the next usable id.
   */
  private String getNextId() {
    long next = this.jedis.incr("nextTweetId");
    return String.valueOf(next);
  }


  @Override
  public void addTweet(Tweet t) {
    this.addTweet(t, false);
  }

  @Override
  public void addTweet(String userId, Calendar datetime, String message)
      throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addTweets(String filePath) {
    this.addTweets(filePath, false);
  }

  /**
   * Parses a Tweet from a {@link JsonReader} and inserts it into the database.
   *
   * @param reader the reader to read the json  from.
   */
  private void addTweetHelp(JsonReader reader, boolean broadcast) throws IOException {
    String userId = null;
    long datetime = -1;
    String message = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("user_id")) {
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
    if (userId == null || datetime == -1 || message == null) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    Calendar c = Calendar.getInstance();
    c.setTime(new Date(datetime));
    Tweet t = new Tweet(userId, c, message);
    this.addTweet(t, broadcast);
  }

  @Override
  public void addTweet(Tweet t, boolean broadcast) {
    String key = "tweet:" + this.getNextId();
    String datetime = sdf.format(t.getDatetime().getTime());
    String value = t.getUserId() + ":" + datetime + ":" + t.getMessage();
    this.jedis.set(key, value);

    if (broadcast) {
      String userId = t.getUserId();
      long timeInMilliseconds = t.getDatetime().getTimeInMillis();
      Set<String> followers = this.jedis.smembers("followers:" + userId);
      for (String s : followers) {
        String tempKey = "hometl:" + s;
        this.jedis.zadd(tempKey, timeInMilliseconds, value);
      }
    }
  }

  @Override
  public void addTweets(String filePath, boolean broadcast) {
    if (filePath == null) {
      throw new IllegalArgumentException("Given file path is null");
    }
    try {
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      while (reader.hasNext()) {
        this.addTweetHelp(reader, broadcast);
      }
      reader.endArray();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
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
    if (filePath == null) {
      throw new IllegalArgumentException("Given file path is null");
    }
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
    return this.getHomeTM(userId, 10);
  }

  @Override
  public List<Tweet> getHomeTM(String userId, int numOfTweets) {
    Set<String> homeTM = this.jedis.zrevrange("hometl:" + userId, 0, numOfTweets);
    Function<String, Tweet> f = new Function<String, Tweet>() {
      @Override
      public Tweet apply(String s) {
        Tweet t = null;
        try {
          String[] data = s.split(":");
          String user = data[0];
          Calendar datetime = Calendar.getInstance();
          datetime.setTime(sdf.parse(data[1]));
          String message = data[2];
          t = new Tweet(user, datetime, message);
        } catch (ParseException e) {
          e.printStackTrace();
        }
        if (t == null) {
          throw new IllegalStateException("Missing data from tweet");
        }
        return t;
      }
    };
    List<Tweet> result = homeTM.stream().map(f).collect(Collectors.toList());
    return result;
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
