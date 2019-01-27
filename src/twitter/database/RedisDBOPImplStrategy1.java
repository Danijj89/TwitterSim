package twitter.database;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents an implementation of the Redis database operations for the
 * Twitter project (strategy 1).
 * Naming conventions used in this implementation:
 * 'tweet:(int)' is the key to access each tweet object;
 * 'user:tweets:(user_id)' is the key for all the tweets of a user;
 * 'followed:(user_id)' is the format of keys for the set user followed by each user;
 *
 * DESIGN:
 * The implementation keeps track of an index counter and utilizes its values to represent unique
 * tweets id's whenever a tweet is added. Each tweet is added using the above naming convention
 * and stores as its values a Redis hash (best way to represent objects). Each user keeps track of
 * all its tweets using a Redis sorted set of tweet id's. To retrieve the home timeline, the program
 * computes the Redis set of followed users and for each of them, gets the top N number of
 * tweets and puts them into a temp list. Then the list gets sorted and the first N number gets
 * retrieved.
 */
public class RedisDBOPImplStrategy1 extends AbstractRedisDBOPImpl {

  /**
   * Establishes a new connection to the local default Redis DB upon construction.
   * Sets up the {@code nextTweetId} index to start at 1.
   * Initializes the format of datetime stored in the database.
   */
  public RedisDBOPImplStrategy1(String datetimeFormat) {
    super(datetimeFormat);
  }

  @Override
  public void addTweet(Tweet t, boolean broadcast) {
    this.addTweet(t);
  }

  @Override
  public void addTweet(Tweet t) {
    this.checkNulls(t);
    String tweetKey = "tweet:" + this.getNextId();
    String datetime = this.sdf.format(t.getDatetime().getTime());
    Map<String, String> values = new HashMap<>();
    values.put("userid", t.getUserId());
    values.put("datetime", datetime);
    values.put("text", t.getMessage());
    this.jedis.hmset(tweetKey, values);
    String userKey = "user:tweets:" + t.getUserId();
    long timeInMilliseconds = t.getDatetime().getTimeInMillis();
    this.jedis.zadd(userKey, timeInMilliseconds, tweetKey);
  }

  @Override
  public void addFollower(String followerId, String followeeId) {
    this.checkNulls(followerId, followeeId);
    String key = "followed:" + followeeId;
    String value = followerId;
    this.jedis.sadd(key,value);
  }

  @Override
  public List<Tweet> getHomeTM(String userId, int numOfTweets) {
    this.checkNulls(userId);
    Set<String> followed = this.getFollowed(userId);

    // List of tweets from all the users that the given user follows.
    // The number of tweets per user retrieved = numOfTweets
    List<Tweet> tempListResult = new ArrayList<>();

    // Function that takes in a user id from the list of those followed by the homeTM user
    // and converts into a tweet and adds it into the tempListResult
    Consumer<String> c = s -> {
      Set<String> tweetIds = this.jedis.zrevrange("user:tweets:" + s, 0, numOfTweets - 1);
      // Function that converts the tweet of a user from string to tweet
      Function<String, Tweet> f2 = s2 -> {
        Tweet t = null;
        try {
          String id = this.jedis.hget(s2, "userid");
          String dt = this.jedis.hget(s2, "datetime");
          String text = this.jedis.hget(s2, "text");
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
      List<Tweet> userTweets = tweetIds.stream().map(f2).collect(Collectors.toList());
      tempListResult.addAll(userTweets);
    };
    Iterator<String> iter = followed.iterator();
    while (iter.hasNext()) {
      c.accept(iter.next());
    }
    // The comparison is inverted such that the sorted array has the highest numbers in the front
    Collections.sort(tempListResult, new Comparator<Tweet>() {
      @Override
      public int compare(Tweet o1, Tweet o2) {
        long t1 = o1.getDatetime().getTimeInMillis();
        long t2 = o2.getDatetime().getTimeInMillis();
        if (t1 > t2) {
          return -1;
        }
        else if (t1 < t2) {
          return 1;
        }
        else {
          return 0;
        }
      }
    });
    // Get the first numOfTweets from the temp array
    List<Tweet> result = new ArrayList<>();
    for (int i = 0; i < numOfTweets && i < tempListResult.size(); i++) {
      result.add(tempListResult.get(i));
    }
    return result;
  }

  /**
   * Not needed for this strategy.
   */
  @Override
  public Set<String> getFollowers(String userId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getFollowed(String userId) {
    Set<String> followees = this.jedis.smembers("followed:" + userId);
    return followees;
  }
}
