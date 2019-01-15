package twitter.database;


/**
 * Value class that represents a Tweet in Twitter.
 */
public class Tweet {

  private final long tweetId;
  private final int userId;
  private final String datetime;
  private String message;

  Tweet(long tweetId, int userId, String datetime, String message) throws IllegalArgumentException {
    if (datetime == null || message == null) {
      throw new IllegalArgumentException("Given datetime or message is null");
    }
    this.tweetId = tweetId;
    this.userId = userId;
    this.datetime = datetime;
    this.message = message;
  }
}
