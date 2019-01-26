package twitter.database;

import com.google.gson.stream.JsonReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Represents an implementation of database operations specific for MySQL for the Twitter project.
 */
public class MySQLDatabaseOPImpl implements MySQLDatabaseOP {

  private Connection connection = null;
  private Statement statement = null;
  private PreparedStatement preparedStatement = null;
  private ResultSet resultSet = null;

  @Override
  public void addTweet(Tweet t) {
    if (t == null) {
      throw new IllegalArgumentException("Given tweet is null");
    }
    this.addTweet(t.getUserId(), t.getDatetime(), t.getMessage());
  }

  @Override
  public void addTweet(String userId, Calendar datetime, String message)
      throws IllegalArgumentException {
    if (userId == null || datetime == null || message == null) {
      throw new IllegalArgumentException("Given datetime or message is null");
    }
    try {
      if (!this.connection.isClosed()) {
        this.preparedStatement = this.connection.prepareStatement(
            "INSERT INTO tweets(user_id,tweet_ts,tweet_text) VALUES (?,?,?)");
        this.preparedStatement.setString(1, userId);
        this.preparedStatement.setString(2, this.dateToMySQLDatetime(datetime));
        this.preparedStatement.setString(3, message);
        this.preparedStatement.executeUpdate();
      }
      else {
        throw new IllegalStateException("Connection is closed");
      }
    } catch (SQLException e) {
      e.getErrorCode();
    }
  }

  @Override
  public void addTweets(String filePath) {
    try {
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      while (reader.hasNext()) {
        this.addTweetHelp(reader);
      }
      reader.endArray();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void addFollower(String followerId, String followeeId) {
    if (followerId.equals(followeeId)) {
      throw new IllegalArgumentException("Follower and followee have the same id");
    }
    try {
      if (!this.connection.isClosed()) {
        this.preparedStatement = this.connection.prepareStatement(
            "INSERT INTO followers(user_id,follows_id) VALUES (?,?)");
        this.preparedStatement.setString(1, followerId);
        this.preparedStatement.setString(2, followeeId);
        this.preparedStatement.executeUpdate();
      }
    } catch (SQLException e) {
      e.getErrorCode();
    }
  }

  @Override
  public void addFollowers(String filePath) {
    try {
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      while (reader.hasNext()) {
        this.addFollowerHelp(reader);
      }
      reader.endArray();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Parses a follower-followee relation from a {@link JsonReader} and inserts it into the database.
   *
   * @param reader the reader to read the json from.
   */
  private void addFollowerHelp(JsonReader reader) throws IOException {
    String follower_id = null;
    String followee_id = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("user_id")) {
        follower_id = reader.nextString();
      }
      else if (name.equals("follows_id")) {
        followee_id = reader.nextString();
      }
      else {
        reader.skipValue();
      }
    }
    reader.endObject();
    if (follower_id == null || followee_id == null) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    this.addFollower(follower_id, followee_id);
  }

  /**
   * Parses a Tweet from a {@link JsonReader} and inserts it into the database.
   *
   * @param reader the reader to read the json  from.
   */
  private void addTweetHelp(JsonReader reader) throws IOException {
    // might be able to remove the tweet_id condition
    String userId = null;
    long datetime = -1;
    String message = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("user_id")) {
        userId = reader.nextString();
      }
      else if (name.equals("datetime")) {
        datetime = reader.nextLong();
      }
      else if (name.equals("message")) {
        message = reader.nextString();
      }
      else {
        reader.skipValue();
      }
    }
    reader.endObject();
    if (userId == null || datetime == -1 || message == null) {
      throw new IllegalStateException("Missing data from current JsonReader");
    }
    Calendar c = Calendar.getInstance();
    c.setTime(new Date(datetime));
    this.addTweet(userId, c, message);
  }

  @Override
  public List<Tweet> getHomeTM(String userId) {
    return this.getHomeTM(userId, 10);
  }

  @Override
  public List<Tweet> getHomeTM(String userId, int numOfTweets) {
    List<Tweet> homeTM = new ArrayList<>();
    try {
      this.resultSet = this.statement.executeQuery(
          "SELECT * FROM tweets JOIN followers on (tweets.user_id = followers.follows_id) "
              + "WHERE followers.user_id = " + String.valueOf(userId) + " ORDER BY tweet_ts "
              + "DESC LIMIT " + String.valueOf(numOfTweets));
      while (this.resultSet.next()) {
        String user = this.resultSet.getString("user_id");
        Timestamp datetime = this.resultSet.getTimestamp("tweet_ts");
        String message = this.resultSet.getString("tweet_text");
        long millis = datetime.getTime();
        Calendar c = Calendar.getInstance();
        c.setTime(new Date(millis));
        Tweet t = new Tweet(user, c, message);
        homeTM.add(t);
      }
    } catch (SQLException e) {
      e.getErrorCode();
    }
    return homeTM;
  }

  @Override
  public Set<String> getFollowers(String userId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetDatabase() {
    try {
      this.statement.executeQuery("TRUNCATE TABLE tweets");
      this.statement.executeQuery("TRUNCATE TABLE followers");
    } catch (SQLException e) {
      e.getErrorCode();
    }
  }

  public void connect(String driver, String connectionPath) throws IllegalStateException {
    try {
      // Setup the driver
      Class.forName(driver).newInstance();

      // Setup the connection with the DB
      this.connection = DriverManager.getConnection(connectionPath);

      // Statements allow to issue SQL queries to the database
      this.statement = this.connection.createStatement();

    } catch (Exception e) {
      throw new IllegalStateException(e.getMessage());
    }
  }

  public void closeConnection() throws IllegalStateException {
    try {
      if (this.resultSet != null) {
        this.resultSet.close();
      }
      if (this.statement != null) {
        this.statement.close();
      }
      if (this.connection != null) {
        this.connection.close();
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Closed them all: Should never happen");
    }
  }

  /**
   * Converts the given {@link Calendar} to a DATETIME string recognized by MySQL.
   *
   * @param t the given datetime.
   * @return the formatted mysql DATETIME string.
   */
  private String dateToMySQLDatetime(Calendar t) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date d = t.getTime();
    return sdf.format(d);
  }
}
