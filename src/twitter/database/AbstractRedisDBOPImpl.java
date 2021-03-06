package twitter.database;

import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import redis.clients.jedis.Jedis;

/**
 * Class that abstracts out the similarities between different strategies for implementing the
 * twitter Redis DB operations.
 */
public abstract class AbstractRedisDBOPImpl implements RedisTwitterDatabaseOP {

  protected final Jedis jedis;
  protected final SimpleDateFormat sdf;

  /**
   * Establishes a new connection to the local default Redis DB upon construction.
   * Sets up the {@code nextTweetId} index to start at 1.
   * Initializes the format of datetime stored in the database.
   */
  public AbstractRedisDBOPImpl(String datetimeFormat) {
    if (datetimeFormat == null) {
      throw new IllegalArgumentException("Given argument is null");
    }
    this.jedis = new Jedis("localhost");
    this.sdf = new SimpleDateFormat(datetimeFormat);
  }

  /**
   * Gets the next usable id to use as tweet id and increments the counter.
   *
   * @return the next usable id.
   */
  protected String getNextId() {
    long next = this.jedis.incr("nextTweetId");
    return String.valueOf(next);
  }

  @Override
  abstract public void addTweet(Tweet t, boolean broadcast);

  @Override
  public void addTweets(String filePath, boolean broadcast) {
    this.checkNulls(filePath);
    try {
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      int counter = 0;
      while (reader.hasNext()) {
        if (counter % 1000 == 0) { System.out.println(counter);}
        this.addTweetHelp(reader, broadcast);
        counter++;
      }
      reader.endArray();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void addTweet(Tweet t) {
    this.checkNulls(t);
    this.addTweet(t, false);
  }

  @Override
  public void addTweet(String userId, Calendar datetime, String message) {
    this.checkNulls(userId, datetime, message);
    Tweet t = new Tweet(userId, datetime, message);
    this.addTweet(t, false);
  }

  @Override
  public void addTweets(String filePath) {
    this.checkNulls(filePath);
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
  abstract public void addFollower(String followerId, String followeeId);

  @Override
  public void addFollowers(String filePath) {
    this.checkNulls(filePath);
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
    this.checkNulls(userId);
    return this.getHomeTM(userId, 10);
  }

  @Override
  abstract public List<Tweet> getHomeTM(String userId, int numOfTweets);

  @Override
  abstract public Set<String> getFollowers(String userId);

  @Override
  abstract public Set<String> getFollowed(String userId);

  @Override
  public void resetDatabase() {
    this.jedis.flushDB();
  }

  /**
   * Helper method to check for null values in a given list of strings.
   *
   * @param s the list of arguments to check.
   */
  protected void checkNulls(Object... s) {
    for (Object arg : s) {
      if (arg == null) {
        throw new IllegalArgumentException("Given argument is null");
      }
    }
  }

  @Override
  public void closeConnection() {
    this.jedis.close();
  }
}
