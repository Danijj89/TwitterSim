package twitter.database;

import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents an implementation of the Redis database operations for the
 * Twitter project (strategy 1).
 * Naming conventions used in this implementation:
 * 'tweet:(int)' is the key for each tweet in the database;
 * 'nextTweetId' is a counter that keeps track of the next usable id to use as tweet id;
 * 'followers:(user_id)' is the format of keys for the set of followers of each user;
 * 'hometl:(user_id)' is the format of keys for the sorted list of tweets in the user home timeline;
 *
 * DESIGN:
 * The implementation keeps track of an index counter and utilizes its values to represent unique
 * tweets id's whenever a tweet is added. Each tweet is added using the above naming convention
 * and stores as its values a Redis hash (best way to represent objects). If the broadcast values
 * is true, it computes the list of followers of the tweet author and adds this tweet to
 * each of those followers' home timeline. The home timeline are represented as Redis sorted set
 * using the date (in milliseconds from epoch) of tweet as the sorting value.
 * Each value in the sorted set is a the key to retrieve the tweet.
 */
public class RedisDatabaseOPImpl extends AbstractRedisDBOPImpl {

  /**
   * Establishes a new connection to the local default Redis DB upon construction.
   * Sets up the {@code nextTweetId} index to start at 1.
   * Initializes the format of datetime stored in the database.
   */
  public RedisDatabaseOPImpl(String datetimeFormat) {
    super(datetimeFormat);
  }

  /**
   * Adds tweets into the DB as hashes (like objects) with the following fields:
   * userid = the id of the user;
   * datetime = the date and time in the format specified when this instance was constructed;
   * text = the message in the tweet;
   */
  @Override
  public void addTweet(Tweet t, boolean broadcast) {
    String key = "tweet:" + this.getNextId();

    String datetime = this.sdf.format(t.getDatetime().getTime());
    Map<String, String> values = new HashMap<>();
    values.put("userid", t.getUserId());
    values.put("datetime", datetime);
    values.put("text", t.getMessage());
    this.jedis.hmset(key, values);

    if (broadcast) {
      String userId = t.getUserId();
      long timeInMilliseconds = t.getDatetime().getTimeInMillis();
      Set<String> followers = this.jedis.smembers("followers:" + userId);
      for (String s : followers) {
        String tempKey = "hometl:" + s;
        this.jedis.zadd(tempKey, timeInMilliseconds, key);
      }
    }
  }

  @Override
  public void addFollower(String followerId, String followeeId) {
    String key = "followers:" + followeeId;
    String value = followerId;
    this.jedis.sadd(key,value);
  }

  @Override
  public List<Tweet> getHomeTM(String userId, int numOfTweets) {
    Set<String> homeTM = this.jedis.zrevrange("hometl:" + userId, 0, numOfTweets);
    // Converts the tweet from a string to a Tweet
    Function<String, Tweet> f = s -> {
        Tweet t = null;
        try {
          String id = this.jedis.hget(s, "userid");
          String dt = this.jedis.hget(s, "datetime");
          String text = this.jedis.hget(s, "text");
          Calendar datetime = Calendar.getInstance();
          datetime.setTime(sdf.parse(dt));
          t = new Tweet(id, datetime, text);
        } catch (ParseException e) {
          e.printStackTrace();
        }
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
    Set<String> followers = this.jedis.smembers("followers:" + userId);
    return followers;
  }

  /**
   * Not needed in this strategy.
   */
  @Override
  public Set<String> getFollowed(String userId) {
    throw new UnsupportedOperationException();
  }
}
