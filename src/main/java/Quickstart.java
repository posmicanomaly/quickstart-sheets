

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.*;

import me.posmicanomaly.quickstart.sheets.Database;

import com.google.api.services.sheets.v4.Sheets;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Quickstart {
	private static final String APPLICATION_NAME = "Google Sheets API Java Quickstart";
	private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"), ".credentials/sheets.googleapis.com-java-quickstart");
	private static FileDataStoreFactory DATA_STORE_FACTORY;
	private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	private static HttpTransport HTTP_TRANSPORT;
	private static final List<String> SCOPES = Arrays.asList(SheetsScopes.SPREADSHEETS);

	static {
		try {
			HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
			DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * Creates an authorized Credential object.
	 * @return an authorized Credential object.
	 * @throws IOException
	 */
	public static Credential authorize() throws IOException {
		// Load client secrets.
		InputStream in = Quickstart.class.getResourceAsStream("/client_secret.json");
		GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

		// Build flow and trigger user authorization request.
		GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
						HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
							.setDataStoreFactory(DATA_STORE_FACTORY)
							.setAccessType("offline")
							.build();
		Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
		System.out.println("Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
		return credential;
	}

	/**
	 * Build and return an authorized Sheets API client service.
	 * @return an authorized Sheets API client service
	 * @throws IOException
	 */
	public static Sheets getSheetsService() throws IOException {
		Credential credential = authorize();
		return new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
				.setApplicationName(APPLICATION_NAME)
				.build();
	}

	public static void printValues(String spreadsheetId, String range) throws IOException {
		Sheets service = getSheetsService();
		ValueRange response = service.spreadsheets().values().get(spreadsheetId, range).execute();
		List<List<Object>> rows = response.getValues();
		if (rows == null || rows.size() == 0) {
			System.out.println("No values found.");
		} else {
			System.out.println("Last, First");
			for (List row : rows) {
				System.out.println(row.get(1) + ", " + row.get(0));
			}
		}
	}

	
	public static List<List<Object>> getDatabaseRecords() {
		List<List<Object>> rows = new ArrayList<>();
	    Connection conn = null;
	    Statement stmt = null;
	    try {
		    conn = Database.getConnection();
		    stmt = conn.createStatement();
		    ResultSet rs = stmt.executeQuery("select * from employee");
		    while(rs.next()) {
		    		List<Object> row = new ArrayList<>();
		    		for(int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
		    			row.add(rs.getObject(i));
		    		}
		    		rows.add(row);
		    }
	    } catch (SQLException e) {
	    		System.out.println(e);
	    } finally {
	    		try { conn.close();	} catch (SQLException e) { System.out.println(e); }
	    }
	    
	    return rows;
	}
	
	public static void writeRowsToSheet(String sheetId, String range, List<List<Object>> rows) {
		ValueRange vr = new ValueRange().setValues(rows).setMajorDimension("ROWS");
    		try {
			getSheetsService().spreadsheets().values().update(sheetId, range, vr).setValueInputOption("RAW").execute();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	public static void main(String[] args) throws IOException {
		String sheetId = "";
		String range = "";
		
		if(sheetId.length() == 0 || range.length() == 0) {
			System.out.println("add sheetId and range in main method");
			System.exit(-1);
		}
		
		System.out.println("Sheet before");
		printValues(sheetId, range);
		System.out.println("Inserting data to sheet from database");
		writeRowsToSheet(sheetId, range, getDatabaseRecords());
		System.out.println("Sheet after");
		printValues(sheetId, range);
	}


}
