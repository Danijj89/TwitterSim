package twitter.database;

import java.util.List;

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
  void addTweet(int userId, String datetime, String message) throws IllegalArgumentException;

  /**
   * Retrieves the home timeline of a a user given his id.
   * The number of tweets in this home timeline is at max 10.
   *
   * @param userId the id of the user.
   * @return the list of tweets in the home timeline of the user.
   */
  List<Tweet> getHomeTM(int userId);

  /**
   * Retrieves the home timeline of a user given his id and the number of tweets in the timeline.
   *
   * @param userId the id of the user.
   * @param numOfTweets the max number of tweets in the user's home timeline.
   * @return the list of tweets in the home timeline of the user.
   */
  List<Tweet> getHomeTM(int userId, int numOfTweets);
}
