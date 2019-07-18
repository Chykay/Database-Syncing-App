import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class DBConnect {
	private static String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());

	public static Connection localDB () throws IOException {
		// Create a variable for the connection string.
		Connection con1 = null;
		String DB = "KokoMD_v2";
		String DBUser = "sa";
		String DBPass = "password";
        String connectionUrl = "jdbc:sqlserver://localhost:1433;" + 
        						"databaseName=" + DB + 
        						";user=" + DBUser + 
        						";password=" + DBPass ; 
        try {
			con1 = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			List<String> lines = Arrays.asList("Error: Got an exception! '"+ e.getMessage() +"' AT: "+ timeStamp);
			SyncTables.Slog(lines);
		} 
        return con1;
	}
	
	public static Connection OnlineDB() throws IOException {
		// Create a variable for the connection string.
		Connection con2 = null;
		String DB = "KokoMD";
		String DBUser = "koemrusr";
		String DBPass = "P@ssword247!";
        String connectionUrl = "jdbc:sqlserver://37.220.93.246:1334;" + 
        						"databaseName=" + DB + 
        						";user=" + DBUser + 
        						";password=" + DBPass ; 
        try {
			con2 = DriverManager.getConnection(connectionUrl);
		} catch (SQLException e) {
			List<String> lines = Arrays.asList("Error: Got an exception! '"+ e.getMessage() +"' AT: "+ timeStamp);
			SyncTables.Slog(lines);
		} 
        return con2;
	}
	
}
