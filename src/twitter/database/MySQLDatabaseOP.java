package twitter.database;

public interface MySQLDatabaseOP extends DatabaseOP {

  void connect(String driver, String connectionPath);

  void closeConnection();

}
