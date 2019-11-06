package syncapp;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class SyncApp {

    private static final ArrayList<String> FFields = new ArrayList<>(); // ArrayList for foreign fields
    private static final ArrayList<String> RTables = new ArrayList<>(); // ArrayList for reference tables 
    private static final ArrayList<String> RPFields = new ArrayList<>(); // ArrayList for reference primary keys
    private static final ArrayList<String> RefCheck = new ArrayList<>(); // ArrayList for reference table record 
    private static final HashMap<String, String> tabledata = new HashMap<>();
    // Hash Map for table columns(key) and data(value) row n, where n = 1,2,3...
    private static boolean checkFK; // Boolean to hold table foreign key status 
    private static final int OrgID = 13; // Company Organisation-ID - used to differentiate company record on the online database
    private static final String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
    public static Path file = Paths.get("SLog.txt"); // Create this file in project directory

    private static Statement localConnection() throws SQLException, IOException, ClassNotFoundException {
        // returns local DB connection
        Connection local = DBConnect.localDB();
        return local.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    private static Statement onlineConnection() throws SQLException, IOException, ClassNotFoundException {
        // returns online DB connection
        Connection Online = DBConnect.OnlineDB();
        return Online.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    public static void Slog(List<String> lines) throws IOException {
        Files.write(file, lines, Charset.forName("UTF-8"), StandardOpenOption.APPEND);
    }

    private static ResultSet getTables() throws SQLException, IOException , ClassNotFoundException{
        // returns foreign key fields in table if any
        String SQL = "SELECT table_name FROM SyncTables";
        ResultSet getTables = onlineConnection().executeQuery(SQL);
        return getTables;
    }

    private static boolean checkforeignkey(String localtable) throws SQLException, IOException, ClassNotFoundException {
        // Checks and returns foreign key fields if any
        FFields.clear();
        RTables.clear();
        RPFields.clear();
        boolean hasFK = false;
        ResultSet CheckFKRS = getforeignKeyFields(localtable); // gets foreign key fields if any
        ResultSetMetaData metaData = CheckFKRS.getMetaData();
        int count = metaData.getColumnCount();
        if (CheckFKRS.isBeforeFirst()) {
            hasFK = true;
            while (CheckFKRS.next()) {
                for (int i = 1; i <= count; i++) {
                    FFields.add(CheckFKRS.getString(i)); // adds foreign key fields to array
                }
            }
            // Get Foreign Key reference table
            ResultSet getRTable = getForeignReferenceTables(localtable);
            ResultSetMetaData metaData1 = getRTable.getMetaData();
            int count1 = metaData1.getColumnCount();
            while (getRTable.next()) {
                for (int j = 1; j <= count1; j++) {
                    RTables.add(getRTable.getString(j)); // adds reference table(s) to array
                    // Get reference tables primary key
                    ResultSet getRPKey = getRefPrimaryKey(getRTable.getString(j));
                    ResultSetMetaData metaData2 = getRPKey.getMetaData();
                    int count2 = metaData2.getColumnCount();
                    while (getRPKey.next()) {
                        for (int k = 1; k <= count2; k++) {
                            RPFields.add(getRPKey.getString(k)); // adds reference primary keys to array
                        }
                    }
                }
            }
        }
        return hasFK;
    }

    private static ResultSet getforeignKeyFields(String table) throws SQLException, IOException, ClassNotFoundException {
        // returns foreign key fields in table if any
        String SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='" + table + "' AND left(CONSTRAINT_NAME,2)='FK'";
        ResultSet foreignRS = localConnection().executeQuery(SQL);
        return foreignRS;
    }

    private static ResultSet getForeignReferenceTables(String table) throws SQLException, IOException, ClassNotFoundException {
        // returns reference table(s)
        String SQL = "SELECT object_name(referenced_object_id) RefTableName FROM sys.foreign_keys WHERE parent_object_id = object_id('" + table + "')";
        ResultSet FRefRS = onlineConnection().executeQuery(SQL);
        return FRefRS;
    }

    private static ResultSet getRefPrimaryKey(String table) throws SQLException, IOException, ClassNotFoundException {
        // returns reference primary keys
        String SQL = "SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1"
                + " AND table_name = '" + table + "'";
        ResultSet RefPK = onlineConnection().executeQuery(SQL);
        return RefPK;
    }

    private static ResultSet getLocalPrimaryKey(String table) throws SQLException, IOException, ClassNotFoundException {
        // returns primary key name from table
        String SQL = "SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1"
                + " AND table_name = '" + table + "'";
        ResultSet RefPK = localConnection().executeQuery(SQL);
        return RefPK;
    }

    private static ResultSet getLocalTableData(String table) throws SQLException, IOException, ClassNotFoundException {
        // returns data ResultSet from table
        String SQL = "SELECT * FROM [dbo]."+table+" WHERE sync = 0 OR sync = null";
        ResultSet localRS = localConnection().executeQuery(SQL);
        return localRS;
    }

    private static ResultSet getNewRefPKey(String table, String PKey, String FLocalID) throws SQLException, IOException, ClassNotFoundException {
        // returns primary key from table using local ID
        String SQL = "SELECT " + PKey + " FROM " + table + " WHERE localID = " + FLocalID;
        //System.out.println("SQL :"+SQL);
        ResultSet RefPK = onlineConnection().executeQuery(SQL);
        return RefPK;
    }

    private static boolean checkOnlineTable(String table, String localID) throws SQLException, IOException, ClassNotFoundException {
        // checks online table if record exists and returns boolean
        if ("".equals(localID)) {
            return false;
        } else {
            String SQL = "SELECT localID FROM " + table + " WHERE localID = " + localID + " AND OrgID = " + OrgID;
            ResultSet checker = onlineConnection().executeQuery(SQL);
            return checker.isBeforeFirst();
        }
    }

    private static ResultSet checkreference(String table, String PKey) throws SQLException, IOException, ClassNotFoundException {
        // gets online reference table record
        String SQL = "SELECT localID FROM " + table + " WHERE localID = " + PKey + " AND OrgID = " + OrgID;
        ResultSet checker = onlineConnection().executeQuery(SQL);
        return checker;
    }

    private static int synchOnlineInsert(String table, String localID) throws SQLException, IOException, ClassNotFoundException {
        // sync table by table insert
		String column = "";
		String value = "";
		for (String i : tabledata.keySet()) {
			column += i +", ";
		}
		for (String i : tabledata.keySet()) {
                    String str = tabledata.get(i);
                    if(str != null && str.contains("'")){
                        str = str.replace("'", "''");
                    }
                    value += "NullIf('"+str+"', 'null'), ";
		}
                
		System.out.println("Column :"+column);
		System.out.println("Value :"+value);
		System.out.println("LocalID :"+localID);
		System.out.println("OrgID :"+OrgID);
		String SQL = "INSERT INTO "+ table +"("+ column +"localID,OrgID) VALUES ("+ value +"'"+ localID +"',"+ OrgID +")";
                System.out.println("SQL :"+SQL);
		int RefPK = onlineConnection().executeUpdate(SQL);
		return RefPK;
    }

    private static int synchOnlineUpdate(String table, String localID) throws SQLException, IOException, ClassNotFoundException {
        // sync table by table update
        String update = "";
        for (String i : tabledata.keySet()) {
            String str = tabledata.get(i);
            if(str != null && str.contains("'")){
                str = str.replace("'", "''");
            }
              update = update + i +" = NullIf('"+ str +"', 'null'), ";
        }
        update = update.substring(0, update.length() - 2);
        String SQL = "UPDATE "+ table +" SET "+ update +" WHERE localID = "+localID+" AND OrgID = "+OrgID;
        int RefPK = onlineConnection().executeUpdate(SQL);
        return RefPK;
    }

    public static int setsynchstatus(String table, String PKField, String PKey) throws SQLException, IOException, ClassNotFoundException {
        // update synch status locally
        String SQL = "Update [dbo].["+ table +"] Set [sync] = 1 where "+ PKField +" = "+PKey;
        int RefPK = localConnection().executeUpdate(SQL);
        return RefPK;
    }

    public static void main(String[] args) throws Exception, IOException, ClassNotFoundException, NullPointerException, IndexOutOfBoundsException {
        try {
            String localtable;
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            ResultSet getTables = getTables();

            List<String> empty = Arrays.asList("----- Start Program log");
            Files.write(file, empty, Charset.forName("UTF-8")); // should clear the log file

            while (getTables.next()) {
                // loops through tables to be synced
                localtable = getTables.getString("table_name");
                //System.out.println("table :"+localtable);
                List<String> lines = Arrays.asList("Syncing Table '" + localtable + "' AT: " + timeStamp);
                Slog(lines);

                checkFK = checkforeignkey(localtable); // checks if table has foreign key
                String PKeyField = null;
                ResultSet getlocaldata = getLocalTableData(localtable); // gets table data
                ResultSetMetaData metaData3 = getlocaldata.getMetaData();
                int count3 = metaData3.getColumnCount();
                while (getlocaldata.next()) {
                    tabledata.clear();
                    RefCheck.clear();
                    for (int k = 1; k <= count3; k++) {
                        tabledata.put(metaData3.getColumnName(k), getlocaldata.getString(k)); // adds table data to HashMap
                    }
                    String PKeyF = "";
                    ResultSet CheckPKey = getLocalPrimaryKey(localtable); // check if table has primary key(which is assumed to be auto increment)
                    while (CheckPKey.next()) {
                        PKeyF = CheckPKey.getString("column_name");
                    }
                    if (!"".equals(PKeyF)) {
                        tabledata.remove(PKeyF); // removes primary key from HashMap if available
                    }
                    if (checkFK == true) {
                        // perform the following if the table has foreign key(s)
                        for (int i = 0; i < FFields.size()-1; i++) {
                            // gets the new online primary key for current record in loop
                            ResultSet onlinePKeyFields = getNewRefPKey(RTables.get(i), RPFields.get(i), getlocaldata.getString(FFields.get(i)));
                            while (onlinePKeyFields.next()) {
                                // replace reference primary key in foreign key field(s)
                                tabledata.replace(FFields.get(i), onlinePKeyFields.getString(RPFields.get(i)));
                            }
                            // checks if record exist on reference table and store in array
                            ResultSet RecordCheck = checkreference(RTables.get(i), getlocaldata.getString(FFields.get(i)));
                            while (RecordCheck.next()) {
                                // add record to array
                                RefCheck.add(RecordCheck.getString("localID"));
                            }
                        }
                    }

                    if (checkFK == true) {
                        if (FFields.size() != RefCheck.size()) {
                            // basically checks if the size of the foreign keys arrays is same as the data gotten from their reference table(s) array 
                            List<String> error = Arrays.asList("--Error: Foreign key dependency data not available :( ☹  for (foreign)table '" + localtable + "' AT: " + timeStamp);
                            Slog(error);
                            break;
                        }
                    }
                    String PKey = "";
                    boolean check;
                    ResultSet ChPKey = getLocalPrimaryKey(localtable); // gets local primary key
                    ResultSet getRPKey = getLocalPrimaryKey(localtable);
                    if (ChPKey.isBeforeFirst()) {
                        while (getRPKey.next()) {
                            PKeyField = getRPKey.getString("column_name");
                        }
                        PKey = getlocaldata.getString(PKeyField);
                        check = checkOnlineTable(localtable, PKey); //checks if record exist in table using Local ID
                    } else {
                        check = false;
                    }

                    // commence syncing
                    if (check) {
                        //updates record if exists
                        int update = synchOnlineUpdate(localtable, PKey);
                        if (update == 1) {
                            List<String> note = Arrays.asList("Online Sync Update Successful for table '" + localtable + "' AT: " + timeStamp);
                            Slog(note);
                            int synched = setsynchstatus(localtable, PKeyField, PKey); //set status to synched in local table
                            if (synched == 1) {
                                List<String> Stat = Arrays.asList("Local Sync Status updated for table '" + localtable + "' AT: " + timeStamp);
                                Slog(Stat);
                            }
                        } else {
                            List<String> note = Arrays.asList("Online Sync Update Error for table '" + localtable + "' AT: " + timeStamp);
                            Slog(note);
                        }
                    } else {
                        // insert record if it does not exist
                        int insert = synchOnlineInsert(localtable, PKey);
                        if (insert == 1) {
                            List<String> note = Arrays.asList("Online Sync Insert Successful for table '" + localtable + "' AT: " + timeStamp);
                            Slog(note);
                            int synched = setsynchstatus(localtable, PKeyField, PKey); //set status to synched in local table
                            if (synched == 1) {
                                List<String> Stat = Arrays.asList("Local Sync Status updated for table '" + localtable + "' AT: " + timeStamp);
                                Slog(Stat);
                            }
                        } else {
                            List<String> note = Arrays.asList("Online Sync Insert Error for table '" + localtable + "' AT: " + timeStamp);
                            Slog(note);
                        }
                    }
                }
            }
        } catch (ClassNotFoundException | SQLException | IOException | NullPointerException | IndexOutOfBoundsException e) {
            List<String> note = Arrays.asList("Error: " + e + " At " + timeStamp);
            Slog(note);
            throw new Exception(e);
        }
        List<String> note = Arrays.asList("----- End: Program Stopped AT " + timeStamp + System.lineSeparator());
        Slog(note);
    }
}
