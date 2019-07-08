import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

// the following should be noted about this application 
// It assumes that all tables with primary keys are (auto increment) or (identity)
// it uses local ID(localID) - Primary Key of records in the local table - and company organisation ID(Org_ID) on the online database to differenciate each record
// It also assumes and expects that the number of columns in the local table is equal to and same as the number of columns in the online database

public class SyncTables {
	private static ArrayList<String> TablesToBeSynched = new ArrayList<String>();
	private static ArrayList<String> FFields = new ArrayList<String>(); // ArrayList for foreign fields
	private static ArrayList<String> RTables = new ArrayList<String>(); // ArrayList for reference tables 
	private static ArrayList<String> RPFields = new ArrayList<String>(); // ArrayList for reference primary keys
	private static ArrayList<String> RefCheck = new ArrayList<String>(); // ArrayList for reference table record 
	private static HashMap<String, String> tabledata = new HashMap<String, String>(); 
	// Hash Map for table columns(key) and data(value) row n, where n = 1,2,3...
	private static boolean checkFK; // Boolean to hold table foreign key status 
    private static int OrgID = 3; // Company Organisation-ID - used to differentiate company record on the online database
    
	private static Statement localConnection() throws SQLException {
		// returns local DB connection
		DBConnect DB = new DBConnect();
		Connection local = DB.localDB();
		return local.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
	}
	
	private static Statement onlineConnection() throws SQLException{
		// returns online DB connection
		DBConnect DB = new DBConnect();
		Connection Online = DB.OnlineDB();
		return Online.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
	}
	
	private static boolean checkforeignkey(String localtable) throws SQLException {
		// Checks and returns foreign key fields if any
		FFields.clear();
		RTables.clear();
		RPFields.clear();
		boolean hasFK = false;
		ResultSet CheckFKRS = getforeignKeyFields(localtable); // gets foreign key fields if any
		ResultSetMetaData metaData = CheckFKRS.getMetaData(); 
		int count = metaData.getColumnCount();
		if(CheckFKRS.isBeforeFirst()) {
			hasFK = true;
			while(CheckFKRS.next()) {
				for (int i = 1; i <= count; i++){
				FFields.add(CheckFKRS.getString(i)); // adds foreign key fields to array
				}
			}
			// Get Foreign Key reference table
			ResultSet getRTable = getForeignReferenceTables(localtable);
			ResultSetMetaData metaData1 = getRTable.getMetaData();
			int count1 = metaData1.getColumnCount();
			while(getRTable.next()) {
				for (int j = 1; j <= count1; j++)
				{
					RTables.add(getRTable.getString(j)); // adds reference table(s) to array
					// Get reference tables primary key
					ResultSet getRPKey = getRefPrimaryKey(getRTable.getString(j));
					ResultSetMetaData metaData2 = getRPKey.getMetaData();
					int count2 = metaData2.getColumnCount();
					while(getRPKey.next()) {
						for (int k = 1; k <= count2; k++)
						{
							RPFields.add(getRPKey.getString(k)); // adds reference primary keys to array
						}
					}
				}
			}
		}
		return hasFK;
	}

	private static ResultSet getforeignKeyFields(String table) throws SQLException {
		// returns foreign key fields in table if any
		String SQL = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='"+ table +"' AND left(CONSTRAINT_NAME,2)='FK'";
		ResultSet foreignRS = localConnection().executeQuery(SQL);
		return foreignRS;
	}
	
	private static ResultSet getForeignReferenceTables(String table) throws SQLException {
		// returns reference table(s)
		String SQL = "SELECT object_name(referenced_object_id) RefTableName FROM sys.foreign_keys WHERE parent_object_id = object_id('"+ table +"')"; 
		ResultSet FRefRS = onlineConnection().executeQuery(SQL);
		return FRefRS;
	}
	
	private static ResultSet getRefPrimaryKey(String table) throws SQLException {
		// returns reference primary keys
		String SQL = "SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1"
				+ " AND table_name = '" + table + "'";
		ResultSet RefPK = onlineConnection().executeQuery(SQL);
		return RefPK;
	}
	
	private static ResultSet getLocalPrimaryKey(String table) throws SQLException {
		// returns primary key name from table
		String SQL = "SELECT column_name FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1"
				+ " AND table_name = '" + table + "'";
		ResultSet RefPK = localConnection().executeQuery(SQL);
		return RefPK;
	}

	private static ResultSet getLocalTableData(String table) throws SQLException {
		// returns data ResultSet from table
		String SQL = "SELECT * FROM [dbo]."+table+" WHERE [synched] = 0";
		ResultSet localRS = localConnection().executeQuery(SQL);
		return localRS;
	}
	
	private static ResultSet getNewRefPKey(String table, String PKey, String FLocalID) throws SQLException {
		// returns primary key from table using local ID
		String SQL = "SELECT "+ PKey +" FROM "+ table +" WHERE localID = "+FLocalID;
		ResultSet RefPK = onlineConnection().executeQuery(SQL);
		return RefPK;
	}
	
	private static boolean checkOnlineTable(String table, String localID) throws SQLException {
		// checks online table if record exists and returns boolean
		if(localID == "") {
			return false;
		}else {
			String SQL = "SELECT localID FROM "+ table +" WHERE localID = "+localID+" AND OrgID = "+OrgID;
			ResultSet checker = onlineConnection().executeQuery(SQL);
			if(checker.isBeforeFirst()) {
				return true;
			}else{
				return false;
			}
		}
	}
	
	private static ResultSet checkreference(String table, String PKey) throws SQLException {
		// gets online reference table record
		String SQL = "SELECT localID FROM "+ table +" WHERE localID = "+ PKey +" AND OrgID = "+OrgID;
		ResultSet checker = onlineConnection().executeQuery(SQL);
		return checker;
	}
	
	private static int synchOnlineInsert(String table, String localID) throws SQLException {
		// sync table by table insert
		String column = "";
		String value = "";
		for (String i : tabledata.keySet()) {
			column += i +", ";
		}
		for (String i : tabledata.keySet()) {
			value += "'"+tabledata.get(i)+"', ";
		}
		String SQL = "INSERT INTO "+ table +"("+ column +"localID,OrgID) VALUES ("+ value + localID +","+ OrgID +")";
		int RefPK = onlineConnection().executeUpdate(SQL);
		return RefPK;
	}
	
	private static int synchOnlineUpdate(String table, String localID) throws SQLException {
		// sync table by table update
		String update = "";
		for (String i : tabledata.keySet()) {
		      update = update + i +" = '"+ tabledata.get(i) +"', ";
		    }
		update = update.substring(0, update.length() - 2);
		String SQL = "UPDATE "+ table +" SET "+ update +" WHERE localID = "+localID+" AND OrgID = "+OrgID;
		int RefPK = onlineConnection().executeUpdate(SQL);
		return RefPK;
	}
	
	public static int setsynchstatus(String table, String PKField, String PKey) throws SQLException {
		// update synch status locally
		String SQL = "Update [dbo].["+ table +"] Set [Synched] = 1 where "+ PKField +" = "+PKey;
		int RefPK = localConnection().executeUpdate(SQL);
		return RefPK;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		String localtable;
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        TablesToBeSynched.add("tablename"); // add table to be synched to array 'TablesToBeSynched'
        for (int z = 0; z < TablesToBeSynched.size(); z++) {
        	// loops through tables to be synched
	        localtable = TablesToBeSynched.get(z);
	        checkFK = checkforeignkey(localtable); // checks if table has foreign key
			String PKeyField = null;
			ResultSet getlocaldata = getLocalTableData(localtable); // gets table data
			ResultSetMetaData metaData3 = getlocaldata.getMetaData();
			int count3 = metaData3.getColumnCount();
			while(getlocaldata.next()) {
				tabledata.clear();
				RefCheck.clear();
				for (int k = 1; k <= count3; k++) {
					tabledata.put(metaData3.getColumnName(k), getlocaldata.getString(k)); // adds table data to HashMap
				}
				String PKeyF = "";
				ResultSet CheckPKey = getLocalPrimaryKey(localtable); // check if table has primary key(which is assumed to be auto increment)
				while(CheckPKey.next()) {
					PKeyF = CheckPKey.getString("column_name");
				}
				if(PKeyF != "") {
					tabledata.remove(PKeyF); // removes primary key from HashMap if available
				}
				if(checkFK == true) {
					// perform the following if the table has foreign key(s)
					for (int i = 0; i < FFields.size(); i++) {
						// gets the new online primary key for current record in loop
						ResultSet onlinePKeyFields = getNewRefPKey(RTables.get(i), RPFields.get(i), getlocaldata.getString(FFields.get(i)));
						while(onlinePKeyFields.next()) {
							// replace reference primary key in foreign key field(s)
							tabledata.replace(FFields.get(i), onlinePKeyFields.getString(RPFields.get(i)));
						}
						// checks if record exist on reference table and store in array
						ResultSet RecordCheck = checkreference(RTables.get(i), getlocaldata.getString(FFields.get(i)));
						while(RecordCheck.next()) {
							// add record to array
							RefCheck.add(RecordCheck.getString("localID"));
						}
					}
				}
				
				if(checkFK == true) {
					if(FFields.size() != RefCheck.size()){
						// basically checks if the size of the foreign keys arrays is same as the data gotten from their reference table(s) array 
						System.out.println("we got here because some foreign key dependency data is not available :(");
						System.out.println(" -------- We are DONE. THE EnD! -------- ");
						break;
					}
				}
				String PKey = "";
				boolean check;
				ResultSet ChPKey = getLocalPrimaryKey(localtable); // gets local primary key
					ResultSet getRPKey = getLocalPrimaryKey(localtable);
					if(ChPKey.isBeforeFirst()) {
						while(getRPKey.next()) {
							PKeyField = getRPKey.getString("column_name");
						}
						PKey = getlocaldata.getString(PKeyField);
						check = checkOnlineTable(localtable, PKey); //checks if record exist in table using Local ID
					}else {
						check = false;
					}

					// commence synching
					if(check) {
						//updates record if exists
						int update = synchOnlineUpdate(localtable,PKey);
						if(update == 1) {
							System.out.println("Synch: Update Successful");
							int synched = setsynchstatus(localtable,PKeyField,PKey); //set status to synched in local table
							if(synched == 1) {
								System.out.println("Synch Down Successful");
							}
						}else {
							System.err.println("Synch: Update NOT Successful");
						}
					}else {
						// insert record if it does not exist
						int insert = synchOnlineInsert(localtable,PKey);
						if(insert == 1) {
							System.out.println("Synch: Insert Successful");
							int synched = setsynchstatus(localtable,PKeyField,PKey); //set status to synched in local table
							if(synched == 1) {
								System.out.println("Synch Down Successful");
							}
						}else {
							System.err.println("Synch: Insert NOT Successful");
						}
					}
			}
        }
        System.out.println(" -------- We are DONE. THE EnD! -------- ");
	}
}