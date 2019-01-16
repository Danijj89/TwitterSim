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
    this.addTweet(t.getUserId(), t.getDatetime(), t.getMessage());
  }

  @Override
  public void addTweet(int userId, String datetime, String message)
      throws IllegalArgumentException {
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
    } catch (IOException e) {
      e.printStackTrace();
    }
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
