
package syncapp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class DBConnect {

    private static final String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());

    public static Connection localDB() throws IOException, ClassNotFoundException {
        // Create a variable for the connection string.
        Connection con1 = null;
        String DB = "EMROnline";
        String DBUser = "sa";
        String DBPass = "password";
        String connectionUrl = "jdbc:sqlserver://localhost:1433;"
                + "databaseName=" + DB
                + ";user=" + DBUser
                + ";password=" + DBPass;
        try {
            con1 = DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            List<String> lines = Arrays.asList("Error: Got an exception! '" + e.getMessage() + "' AT: " + timeStamp);
            SyncApp.Slog(lines);
        }
        return con1;
    }

    public static Connection OnlineDB() throws IOException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        // Create a variable for the connection string.
        Connection con2 = null;
        String DB = "EMR_Online";
        String DBUser = "kokoremote";
        String DBPass = "P\"ssword247!";
        String connectionUrl = "jdbc:sqlserver://148.72.22.111;"
                + "databaseName=" + DB
                + ";user=" + DBUser
                + ";password=" + DBPass;
        try {
            con2 = DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            List<String> lines = Arrays.asList("Error: Got an exception! '" + e.getMessage() + "' AT: " + timeStamp);
            SyncApp.Slog(lines);
        }
        return con2;
    }

}
