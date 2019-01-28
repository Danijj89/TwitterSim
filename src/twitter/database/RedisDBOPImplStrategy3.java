package twitter.database;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents an implementation of the Redis database operations for the
 * Twitter project (strategy 3).
 * Naming conventions used in this implementation:
 * 'tweet:(int)' is the key to access each tweet object;
 * 'hometl:(user_id)' is the key for the home timeline of the users;
 * 'followers:(user_id)' is the format of keys for the set users that follow the user;
 *
 * DESIGN:
 * The implementation keeps track of an index counter and utilizes its values to represent unique
 * tweets id's whenever a tweet is added. Each tweet is added using the above naming convention
 * and stores as strings. If the broadcast values is true, it computes the list of followers of
 * the tweet author and adds this tweet to each of those followers' home timeline.
 * The home timeline are represented as Redis sorted set using the date (in milliseconds from epoch)
 * of tweet as the sorting value. Each value in the sorted set is a tweet.
 * When retrieving the home timeline, to simulate reading strings from DB without turning the result
 * into tweets, the {@code getHomeTM} method returns an empty list.
 */
public class RedisDBOPImplStrategy3 extends AbstractRedisDBOPImpl {

  /**
   * Establishes a new connection to the local default Redis DB upon construction.
   * Sets up the {@code nextTweetId} index to start at 1.
   * Initializes the format of datetime stored in the database.
   */
  public RedisDBOPImplStrategy3(String datetimeFormat) {
    super(datetimeFormat);
  }

  @Override
  public void addTweet(Tweet t, boolean broadcast) {
    this.checkNulls(t);
    String key = "tweet:" + this.getNextId();
    long timeInMilliseconds = t.getDatetime().getTimeInMillis();
    String datetime = String.valueOf(timeInMilliseconds);
    String values = t.getUserId() + ":" + datetime + ":" + t.getMessage();
    this.jedis.set(key, values);

    if (broadcast) {
      String userId = t.getUserId();
      Set<String> followers = this.jedis.smembers("followers:" + userId);
      for (String s : followers) {
        String tempKey = "hometl:" + s;
        this.jedis.zadd(tempKey, timeInMilliseconds, values);
      }
    }
  }

  @Override
  public void addFollower(String followerId, String followeeId) {
    this.checkNulls(followerId, followeeId);
    String key = "followers:" + followeeId;
    String value = followerId;
    this.jedis.sadd(key,value);
  }

  @Override
  public List<Tweet> getHomeTM(String userId, int numOfTweets) {
    this.checkNulls(userId);
    if (numOfTweets < 1) {
      throw new IllegalArgumentException("The number of tweets has to be bigger than 0");
    }
    Set<String> homeTM = this.jedis.zrevrange("hometl:" + userId, 0, numOfTweets - 1);
    // Converts the tweet from a string to a Tweet
    Function<String, Tweet> f = s -> {
      Tweet t = null;
      List<String> data = Arrays.asList(s.split(":"));
      String id = data.get(0);
      String dt = data.get(1);
      long timeInMilliseconds = Long.valueOf(dt);
      String text = null;
      if (data.size() == 2) {
        text = "";
      }
      else {
        text = data.get(2);
      }
      Calendar datetime = Calendar.getInstance();
      datetime.setTime(new Date(timeInMilliseconds));
      t = new Tweet(id, null, text);
      if (t == null) {
        throw new IllegalStateException("Missing data from tweet");
      }
      return t;
    };
    List<Tweet> result = homeTM.stream().map(f).collect(Collectors.toList());
    return result;
  }

  @Override
  public Set<String> getFollowers(String userId) {
    this.checkNulls(userId);
    Set<String> followers = this.jedis.smembers("followers:" + userId);
    return followers;
  }

  @Override
  public Set<String> getFollowed(String userId) {
    throw new UnsupportedOperationException();
  }
}
