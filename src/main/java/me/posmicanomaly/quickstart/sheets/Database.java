package me.posmicanomaly.quickstart.sheets;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class Database {
	private static String dbURL = "jdbc:derby:derbyDB;create=true;user=root;password=changeme";
	private static Connection conn;
	private static boolean initialized = false;
	
	public static Connection getConnection() throws SQLException {
		try {
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if(Database.conn == null) {
			Database.conn = DriverManager.getConnection(Database.dbURL);
		}
		
		if(!Database.initialized) {
			Database.prepareSchema(conn);
			Database.initialized = true;
		}
		
		return Database.conn;
		
	}
	
	public static void dropTable(Connection conn, String table) throws SQLException {
		conn.createStatement().execute("drop table " + table);
	}
	
	public static void truncateTable(Connection conn, String table) throws SQLException {
		conn.createStatement().execute("truncate table " + table);
	}
	
	public static void executeSQLFile(Connection conn, File f) throws SQLException, FileNotFoundException {
		Scanner in = new Scanner(f);
		StringBuilder sb = new StringBuilder();
		while(in.hasNextLine()) { 
			sb.append(in.nextLine() + System.lineSeparator());
		}
		in.close();
		
		Statement stmt = conn.createStatement();
		stmt.execute(sb.toString());
	}
	
	public static void prepareSchema(Connection conn) {
		try { Database.dropTable(conn, "employee"); } catch (SQLException e) { System.out.println("error dropping table"); }
		try { 
			Database.executeSQLFile(conn, new File("src/main/resources/create_employee.sql")); 
		} catch (SQLException | FileNotFoundException e) { 
			System.out.println("error creating table"); 
		}
		try { Database.truncateTable(conn, "employee"); } catch (SQLException e) { System.out.println("error truncating table"); }
		
		
		// insert mock data
		try {
		PreparedStatement pstmt = conn.prepareStatement("insert into employee (firstname, lastname) values (?, ?)");
		File mockFile = new File("src/main/resources/MOCK_DATA.csv");
		Scanner in = new Scanner(mockFile) ;
		while(in.hasNextLine()) {
			String[] split = in.nextLine().split(",");
			pstmt.setString(1, split[0]);
			pstmt.setString(2, split[1]);
			pstmt.addBatch();
		}
		in.close();
		
		int[] rows = pstmt.executeBatch();
		System.out.println("rows added: " + rows.length);
		} catch (SQLException | FileNotFoundException e) {
			System.out.println("error inserting mock data");
			System.out.println(e);
		}
	}
}
