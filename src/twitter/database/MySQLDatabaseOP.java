package twitter.database;

/**
 * Represents an interface to connect to a MySQL database.
 */
public interface MySQLDatabaseOP extends DatabaseOP {

  /**
   * Connects to a MySQL Database.
   *
   * @param driver the driver.
   * @param connectionPath the connection path to the database.
   */
  void connect(String driver, String connectionPath);

  /**
   * Closes the connection to the database.
   */
  void closeConnection();

}
