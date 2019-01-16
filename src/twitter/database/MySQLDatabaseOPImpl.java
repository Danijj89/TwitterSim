package twitter.database;

import com.google.gson.stream.JsonReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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
  public void addTweet(int userId, String datetime, String message)
      throws IllegalArgumentException {
    if ()
    try {
      this.preparedStatement = this.connection.prepareStatement(
          "INSERT INTO tweets(user_id,tweet_ts,tweet_text) VALUES (?,?,?)");
      this.preparedStatement.setString(1, String.valueOf(userId));
      this.preparedStatement.setString(2, datetime);
      this.preparedStatement.setString(3, message);
      this.preparedStatement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void addTweets(String filePath) {
    try {
      JsonReader reader = new JsonReader(new FileReader(filePath));
      reader.beginArray();
      while (reader.hasNext()) {
        reader.beginArray();
        while (reader.hasNext()) {
          this.addTweetHelp(reader);
        }
        reader.endArray();
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Parses a Tweet from a Json object and inserts it into the database.
   *
   * @param reader the reader to read the json object from.
   */
  private void addTweetHelp(JsonReader reader) throws IOException {
    long tweet_id = -1;
    int userId = -1;
    String datetime = null;
    String message = null;
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equals("tweet_id")) {
        tweet_id = reader.nextLong();
      }
      if (name.equals("user_id")) {
        userId = reader.nextInt();
      }
      if (name.equals("datetime")) {
        datetime = reader.nextString();
      }
      if (name.equals("message")) {
        message = reader.nextString();
      }
      else {
        reader.skipValue();
      }
    }
    reader.endObject();
    this.addTweet(userId, , );
  }

  @Override
  public List<Tweet> getHomeTM(int userId) {
    return null;
  }

  @Override
  public List<Tweet> getHomeTM(int userId, int numOfTweets) {
    return null;
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
}
