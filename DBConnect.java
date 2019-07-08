import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnect {

	public Connection localDB () {
		// Create a variable for the connection string.
		Connection con1 = null;
		String DB = "local/first DB name";
		String DBUser = "user";
		String DBPass = "password";
        String connectionUrl = "jdbc:sqlserver://localhost:1433;" + 
        						"databaseName=" + DB + 
        						";user=" + DBUser + 
        						";password=" + DBPass ; 
        try {
			con1 = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			System.err.println("Got an exception! ");
			System.err.println(e.getMessage());
		} 
        
        return con1;
	}
	
	public Connection OnlineDB() {
		// Create a variable for the connection string.
		Connection con2 = null;
		String DB = "Online/Second DB name";
		String DBUser = "user";
		String DBPass = "password";
        String connectionUrl = "jdbc:sqlserver://35.256.78.345:1334;" + 
        						"databaseName=" + DB + 
        						";user=" + DBUser + 
        						";password=" + DBPass ; 
        try {
			con2 = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			System.err.println("Got an exception! ");
			System.err.println(e.getMessage());
		} 
        
        return con2;
	}
	
}
