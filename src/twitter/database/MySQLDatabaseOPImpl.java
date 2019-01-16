package twitter.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class MySQLDatabaseOPImpl implements DatabaseOP {

  private Connection connection = null;
  private Statement statement = null;
  private PreparedStatement preparedStatement = null;
  private ResultSet resultSet = null;

  @Override
  public void addTweet(Tweet t) {

  }

  @Override
  public void addTweet(int userId, String datetime, String message)
      throws IllegalArgumentException {

  }

  @Override
  public List<Tweet> getHomeTM(int userId) {
    return null;
  }

  @Override
  public List<Tweet> getHomeTM(int userId, int numOfTweets) {
    return null;
  }

  protected void connect(String driver, String connectionPath) throws IllegalStateException {
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

  protected void closeConnection() throws IllegalStateException {
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
