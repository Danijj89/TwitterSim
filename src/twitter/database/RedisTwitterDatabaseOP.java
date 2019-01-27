package twitter.database;

/**
 * Represents an interface of additional operation when using Redis for the Twitter project.
 */
public interface RedisTwitterDatabaseOP extends DatabaseOP {

  /**
   * Adds a given {@link Tweet} into the DB and if broadcast is {@code true}, will
   * broadcast the tweet to all the followers of the tweet author.
   *
   * @param t the tweet to be added.
   * @param broadcast if it has to be broadcasted to the followers.
   */
  void addTweet(Tweet t, boolean broadcast);

  /**
   * Adds all the tweets from a given file path into the DB. If the broadcast value is {@code true},
   * it will broadcast the tweets to all teh followers of each tweet author.
   *
   * @param filePath the path to the file to be read.
   * @param broadcast if it has to be broadcasted to the followers.
   */
  void addTweets(String filePath, boolean broadcast);

  /**
   * Closes the connection to the DB.
   */
  void closeConnection();

}
