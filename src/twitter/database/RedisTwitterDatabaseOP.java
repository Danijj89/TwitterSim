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

}
