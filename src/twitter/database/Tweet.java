package twitter.database;


import java.util.Calendar;

/**
 * Value class that represents a Tweet in Twitter.
 * The max length of the message of this tweet is set to 139.
 * String is the preferred datatype as we are working with databases.
 */
public class Tweet {

  private final String tweetId;
  private final String userId;
  private final Calendar datetime;
  private String message;

  public Tweet(String tweetId, String userId, Calendar datetime, String message) throws IllegalArgumentException {
    if (tweetId == null || userId == null || datetime == null || message == null) {
      throw new IllegalArgumentException("Given datetime or message is null");
    }
    if (message.length() > 139) {
      throw new IllegalArgumentException("Length of the message has to be smaller than 140");
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
  public String getTweetId() {
    return tweetId;
  }

  /**
   * Gets the userId of this Tweet .
   *
   * @return the userId
   */
  public String getUserId() {
    return userId;
  }

  /**
   * Gets the datetime of this Tweet .
   *
   * @return the datetime
   */
  public Calendar getDatetime() {
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
