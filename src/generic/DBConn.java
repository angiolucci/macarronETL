package generic;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConn {

	private static final String DB_DRIVER = "org.postgresql.Driver";
	private static final String DB_CONNECTION = "jdbc:postgresql://localhost/dw_proto2";
	private static final String DB_USER = "postgres";
	private static final String DB_PASSWORD = " ";


	public static Long execute(String query, String idName, long register) {
		Long id = null;
		Connection conn = null;
		Statement stmt = null;
		try {

			// STEP 3: Open a connection
			//System.out.println("Connecting to database...");
			conn = getDBConnection();

			// STEP 4: Execute a query
			//System.out.println("Creating statement...");
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			// STEP 5: Extract data from result set
			while (rs.next()) {
				// Retrieve by column name
				id = rs.getLong(idName);
				// Display values
				//System.out.print("ID: " + id);
			}
			// STEP 6: Clean-up environment
			rs.close();
			stmt.close();
			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
			System.err.println("\nRegister no. " + register);
			System.err.println(query);
			System.exit(-1);

		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
			System.err.println("\nRegister no. " + register);
			System.err.println(query);
			System.exit(-1);
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}// end finally try
		}// end try
		//System.out.println("Goodbye!");
		return id;
	}

	private static Connection getDBConnection() {
		Connection dbConnection = null;
		try {
			Class.forName(DB_DRIVER);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		try {
			dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER,
					DB_PASSWORD);
			return dbConnection;
		} catch (SQLException e) {
			e.printStackTrace();;
			System.exit(-1);
		}
		return dbConnection;
	}
}