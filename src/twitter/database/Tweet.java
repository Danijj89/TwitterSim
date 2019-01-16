package twitter.database;


/**
 * Value class that represents a Tweet in Twitter.
 */
public class Tweet {

  private final long tweetId;
  private final int userId;
  private final String datetime;
  private String message;

  public Tweet(long tweetId, int userId, String datetime, String message) throws IllegalArgumentException {
    if (datetime == null || message == null) {
      throw new IllegalArgumentException("Given datetime or message is null");
    }
    this.tweetId = tweetId;
    this.userId = userId;
    this.datetime = datetime;
    this.message = message;
  }

  /**
   * Gets the tweetId of this Tweet .
   *
   * @return the tweetId
   */
  public long getTweetId() {
    return tweetId;
  }

  /**
   * Gets the userId of this Tweet .
   *
   * @return the userId
   */
  public int getUserId() {
    return userId;
  }

  /**
   * Gets the datetime of this Tweet .
   *
   * @return the datetime
   */
  public String getDatetime() {
    return datetime;
  }

  /**
   * Gets the message of this Tweet .
   *
   * @return the message
   */
  public String getMessage() {
    return message;
  }
}
