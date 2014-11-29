package generic;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class DBConn {

	private static final String DB_DRIVER = "org.postgresql.Driver";
	//private static final String DB_CONNECTION = "jdbc:postgresql://localhost/dw_proto2";
	//private static final String DB_USER = "postgres";
	//private static final String DB_PASSWORD = " ";
	
	
	private Connection dbConnection = null;
	
	
	public Long execute(String query, String idName, long register) {
		
		Long id = null;
		Statement stmt = null;

		try {
			
			stmt = this.dbConnection.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while (rs.next()) {
				id = rs.getLong(idName);
			}
			
			rs.close();
			stmt.close();

		} catch (Exception e) {

			e.printStackTrace();
			System.err.println("\nRegister no. " + register);
			System.err.println(query);
			System.exit(-1);
			
		}

		return id;
	}

	public void close() {
		try {
			
			this.dbConnection.close();
			
		} catch (SQLException e) {
			
			e.printStackTrace();
			
		}
	}

	public DBConn() {
		DBConfigReader reader = new DBConfigReader("dbconn.cfg");
		HashMap<String, String> props= reader.getConnProperties();
		String DB_CONNECTION =  (String) props.get("connection");
		String DB_USER =  (String) props.get("user");
		String DB_PASSWORD =  (String) props.get("password");

		try {
			
			Class.forName(DB_DRIVER);
			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER,
					DB_PASSWORD);
			
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
			System.exit(-1);
			
		} catch (SQLException e) {
			
			e.printStackTrace();
			System.exit(-1);
			
		}

	}
}