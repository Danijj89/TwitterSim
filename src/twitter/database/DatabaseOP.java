package twitter.database;

import java.util.Calendar;
import java.util.List;
import java.util.Set;

/**
 * Represents an interface of operations that interact with a Twitter database.
 */
public interface DatabaseOP {

  /**
   * Adds a given tweet into the Twitter database.
   *
   * @param t the tweet to be added.
   */
  void addTweet(Tweet t);

  /**
   * Adds a new tweet into the Twitter database given a tweet data.
   * The message length has to be between 0 and 140 characters.
   *
   * @param userId the user who wrote the tweet.
   * @param datetime the tweet posting date and time.
   * @param message the tweet message.
   * @throws IllegalArgumentException if any argument is not valid.
   */
  void addTweet(String userId, Calendar datetime, String message) throws IllegalArgumentException;

  /**
   * Adds all the tweets from a given file.
   *
   * @param filePath the path to the file.
   */
  void addTweets(String filePath);

  /**
   * Adds a given follower-followee relation into the Twitter database.
   *
   * @param followerId the id of the follower.
   * @param followeeId the id of the followee.
   */
  void addFollower(String followerId, String followeeId);

  /**
   * Adds all the follower-followee relations from a given file.
   *
   * @param filePath the path to the file.
   */
  void addFollowers(String filePath);

  /**
   * Retrieves the home timeline of a a user given his id.
   * The number of tweets in this home timeline is at max 10.
   *
   * @param userId the id of the user.
   * @return the list of tweets in the home timeline of the user.
   */
  List<Tweet> getHomeTM(String userId);

  /**
   * Retrieves the home timeline of a user given his id and the number of tweets in the timeline.
   *
   * @param userId the id of the user.
   * @param numOfTweets the max number of tweets in the user's home timeline.
   * @return the list of tweets in the home timeline of the user.
   */
  List<Tweet> getHomeTM(String userId, int numOfTweets);

  /**
   * Retrieves all the user's id that follow a given user id.
   *
   * @param userId the id of the followed user.
   * @return the list of id of the followers
   */
  Set<String> getFollowers(String userId);

  /**
   * Resets the database.
   */
  void resetDatabase();
}
