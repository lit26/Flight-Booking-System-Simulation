import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * Runs queries against a back-end database
 */
public class Query
{
	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLPassword;

	// DB Connection
	private Connection conn;

	// Delete table
	private static final String DELETE_STATEMENT = "DELETE FROM RESERVATIONS; DBCC CHECKIDENT (RESERVATIONS, RESEED, 0); DELETE FROM ITINERARIES; DELETE FROM USERS;" +
												  "ALTER TABLE FLIGHTS DROP CONSTRAINT CK_Seat; ALTER TABLE FLIGHTS DROP COLUMN seat; " + 
												  "ALTER TABLE FLIGHTS ADD seat int CONSTRAINT CK_Seat CHECK(seat >= 0)";
	private PreparedStatement deleteStatement;
	private static final String DELETE_ITINERARIES_STATEMENT = "DELETE FROM ITINERARIES WHERE username = ?";
	private PreparedStatement deleteItinerariesStatement;


	// Logged In User
	private String username; // customer username is unique
	private static final String LOG_IN_STATEMENT = "SELECT * FROM USERS WHERE username = ? AND password = ?";
	private PreparedStatement logInStatement;

	// Search if user exist
	private static final String SEARCH_USER_STATEMENT = "SELECT * FROM USERS WHERE username = ?";
	private PreparedStatement searchUserStatement;
	// Create user if not exist
	private static final String CREATE_USER_STATEMENT = "INSERT INTO USERS VALUES (?, ?, ?)";
	private PreparedStatement createUserStatement;

	// Insert itineraries
	private static final String INSERT_ITINERARIES_STATEMENT = "INSERT INTO ITINERARIES VALUES (?, ?, ?, ?, ?, ?)";
	private PreparedStatement insertItinerariesStatement;
	private static final String INSERT_SEAT_STATEMENT1 = "UPDATE FLIGHTS SET seat = capacity WHERE fid = ? AND seat IS null;";
	private PreparedStatement insertSeatStatement1;
	private static final String INSERT_SEAT_STATEMENT2 = "UPDATE FLIGHTS SET seat = capacity WHERE fid = ? AND seat IS null; " + 
														"UPDATE FLIGHTS SET seat = capacity WHERE fid = ? AND seat IS null;";
	private PreparedStatement insertSeatStatement2;

	private static final String DIRECT_FLIGHT_STATEMENT = "SELECT TOP(?) * FROM FLIGHTS AS f " +
														 "WHERE f.origin_city = ? " +
														 "AND f.dest_city = ? " +
														 "AND f.day_of_month = ? " +
														 "AND canceled != 1 "+
														 "ORDER BY f.actual_time";

	private PreparedStatement directFlightStatement;

	private static final String TWO_HOP_STATEMENT = "SELECT TOP(?) (F1.actual_time+F2.actual_time) AS time, F1.fid AS f1_fid, " +
			"F1.day_of_month AS f1_day_of_month, F1.carrier_id AS f1_carrier, F1.flight_num AS f1_flight_num, " +
			"F1.origin_city AS f1_origin_city, F1.dest_city AS f1_dest_city, " + 
			"F1.actual_time AS f1_actual_time, F1.capacity AS f1_capacity,F1.price AS f1_price, " + 
			"F2.fid AS f2_fid, F2.day_of_month AS f2_day_of_month, F2.carrier_id AS f2_carrier, " + 
			"F2.flight_num AS f2_flight_num, F2.origin_city AS f2_origin_city, F2.dest_city AS f2_dest_city, " +
			"F2.actual_time AS f2_actual_time, F2.capacity AS f2_capacity, F2.price AS f2_price " +
			"FROM FLIGHTS AS F1, FLIGHTS AS F2 " +
			"WHERE F1.origin_city = ? " +
			"AND F1.dest_city = F2.origin_city " +
			"AND F2.dest_city = ? " +
			"AND F1.day_of_month = ? " +
			"AND F1.day_of_month = F2.day_of_month " +
			"AND F1.canceled !=1 AND F2.canceled !=1 " +
			"ORDER BY (F1.actual_time + F2.actual_time), F1.fid";
	private PreparedStatement twoHopStatement;
	
	private static final String SEARCH_ITINERARIES_STATEMENT = "SELECT * FROM ITINERARIES " +
															  "WHERE Itineraries_index = ? " +
															  "AND username = ?";
	private PreparedStatement searchItinerariesStatement;
	private static final String SEARCH_ITINERARIES = "SELECT * FROM ITINERARIES WHERE Itineraries_index = ? "+
													 "AND username = ?";
	private PreparedStatement searchItineraries;
	
	
	private static final String SEARCH_USER_BOOK_ALREADY = "SELECT * FROM RESERVATIONS " +
														  "WHERE username = ? AND day = ? AND canceled != 1";
	private PreparedStatement searchUserBookAlready;
	
	private static final String INSERT_RESERVATIONS1 = "INSERT INTO RESERVATIONS (username, f1_fid, f2_fid, day, price) " +
													 "VALUES (?, ?, ?, ?,?); UPDATE FLIGHTS SET seat = seat - 1 WHERE fid = ?;";
	private PreparedStatement insertReservations1;
	private static final String INSERT_RESERVATIONS2 = "INSERT INTO RESERVATIONS (username, f1_fid, f2_fid, day, price) " +
			 										 "VALUES (?, ?, ?, ?,?); UPDATE FLIGHTS SET seat = seat - 1 WHERE fid = ?; UPDATE FLIGHTS SET seat = seat - 1 WHERE fid = ?;";
	private PreparedStatement insertReservations2;
	
	private static final String RETRIEVE_RESERVATION_ID = "SELECT * FROM RESERVATIONS "+
														 "WHERE username = ? AND f1_fid = ? AND f2_fid = ? AND canceled != 1";
	private PreparedStatement retrieveReservationID;
	private static final String RETRIEVE_ALL_RESERVATION = "SELECT * FROM RESERVATIONS "+
			 											  "WHERE username = ? AND canceled != 1";
	private PreparedStatement retrieveAllReservation;
	private static final String RETRIEVE_ID_RESERVATION_PAY = "SELECT * FROM RESERVATIONS "+
			  											 "WHERE id = ? AND username = ? AND canceled != 1 AND pay = 0";
	private PreparedStatement retrieveIdReservationPay;
	private static final String RETRIEVE_ID_RESERVATION_CANCEL = "SELECT * FROM RESERVATIONS "+
				 											 "WHERE id = ? AND username = ? AND canceled != 1";
	private PreparedStatement retrieveIdReservationCancel;
	private static final String SEARCH_FLIGHTS = "SELECT * FROM FLIGHTS WHERE fid = ? AND canceled != 1";
	
	private PreparedStatement searchFlights;
	private static final String PAY_SUCCESS_STATEMENT = "UPDATE RESERVATIONS SET pay = 1 WHERE id = ? AND pay = 0; ";
	private PreparedStatement paySuccessStatement;
	private static final String UPDATE_USER_BALANCE = "UPDATE USERS SET balance = ? WHERE username = ? ";
	private PreparedStatement updateUserBalance;
	private static final String CANCEL_SUCCESS_STATEMENT1= "UPDATE RESERVATIONS SET canceled = 1 WHERE id = ? AND canceled = 0;UPDATE FLIGHTS set seat = seat + 1 " +
														  "WHERE fid = ?;";
	private PreparedStatement cancelSuccessStatement1;
	private static final String CANCEL_SUCCESS_STATEMENT2= "UPDATE RESERVATIONS SET canceled = 1 WHERE id = ? AND canceled = 0;UPDATE FLIGHTS set seat = seat + 1 " +
			  											  "WHERE fid = ?; UPDATE FLIGHTS set seat = seat + 1 WHERE fid = ?;";
	private PreparedStatement cancelSuccessStatement2;

	// Canned queries

	private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
	private PreparedStatement checkFlightCapacityStatement;

	// transactions
	private static final String BEGIN_TRANSACTION_REPEATEDREAD_SQL = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionRepeatedReadStatement;
	private static final String BEGIN_TRANSACTION_SQL = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;

	class Flight
	{
		public int fid;
		public int dayOfMonth;
		public String carrierId;
		public String flightNum;
		public String originCity;
		public String destCity;
		public int time;
		public int capacity;
		public int price;

		public Flight(int fid, int dayOfMonth, String carrierId, String flightNum, String originCity, String destCity,
				int time, int capacity, int price) {
			this.fid = fid;
			this.dayOfMonth = dayOfMonth;
			this.carrierId = carrierId;
			this.flightNum = flightNum;
			this.originCity = originCity;
			this.destCity = destCity;
			this.time = time;
			this.capacity = capacity;
			this.price = price;
		}
		@Override
		public String toString()
		{
			return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId +
					" Number: " + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time +
					" Capacity: " + capacity + " Price: " + price;
		}
	}
	
	class Itineraries
	{
		public int time;
		public Flight f1;
		public Flight f2;
		public Itineraries(int time,Flight f1, Flight f2) {
			this.time = time;
			this.f1 = f1;
			this.f2 = f2;
		}	
	}
	class sortItineraries implements Comparator<Itineraries>{
		@Override
		public int compare(Itineraries a, Itineraries b) {
			int difference = a.time - b.time;
			if(difference == 0) {
				difference = a.f1.fid-b.f1.fid;
			}
			return difference;
		}		
	}

	public Query(String configFilename)
	{
		this.configFilename = configFilename;
	}

	/* Connection code to SQL Azure.  */
	public void openConnection() throws Exception
	{
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver = configProps.getProperty("flightservice.jdbc_driver");
		jSQLUrl = configProps.getProperty("flightservice.url");
		jSQLUser = configProps.getProperty("flightservice.sqlazure_username");
		jSQLPassword = configProps.getProperty("flightservice.sqlazure_password");

		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the flights database */
		conn = DriverManager.getConnection(jSQLUrl, // database
				jSQLUser, // user
				jSQLPassword); // password

		conn.setAutoCommit(true); //by default automatically commit after each statement

		/* You will also want to appropriately set the transaction's isolation level through:
       conn.setTransactionIsolation(...)
       See Connection class' JavaDoc for details.
		 */
	}

	public void closeConnection() throws Exception
	{
		conn.close();
	}

	/**
	 * Clear the data in any custom tables created. Do not drop any tables and do not
	 * clear the flights table. You should clear any tables you use to store reservations
	 * and reset the next reservation ID to be 1.
	 */
	public void clearTables ()
	{
		try {
			deleteStatement.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * prepare all the SQL statements in this method.
	 * "preparing" a statement is almost like compiling it.
	 * Note that the parameters (with ?) are still not filled in
	 */
	public void prepareStatements() throws Exception
	{
		deleteStatement = conn.prepareStatement(DELETE_STATEMENT);
		deleteItinerariesStatement = conn.prepareStatement(DELETE_ITINERARIES_STATEMENT);
		logInStatement = conn.prepareStatement(LOG_IN_STATEMENT);
		searchUserStatement = conn.prepareStatement(SEARCH_USER_STATEMENT);
		searchItineraries = conn.prepareStatement(SEARCH_ITINERARIES);
		createUserStatement = conn.prepareStatement(CREATE_USER_STATEMENT);
		insertItinerariesStatement = conn.prepareStatement(INSERT_ITINERARIES_STATEMENT);
		insertSeatStatement1 = conn.prepareStatement(INSERT_SEAT_STATEMENT1);
		insertSeatStatement2 = conn.prepareStatement(INSERT_SEAT_STATEMENT2);
		directFlightStatement = conn.prepareStatement(DIRECT_FLIGHT_STATEMENT);
		twoHopStatement = conn.prepareStatement(TWO_HOP_STATEMENT);
		searchItinerariesStatement = conn.prepareStatement(SEARCH_ITINERARIES_STATEMENT);
		searchUserBookAlready = conn.prepareStatement(SEARCH_USER_BOOK_ALREADY);
		insertReservations1 = conn.prepareStatement(INSERT_RESERVATIONS1);
		insertReservations2 = conn.prepareStatement(INSERT_RESERVATIONS2);
		retrieveReservationID = conn.prepareStatement(RETRIEVE_RESERVATION_ID);
		retrieveAllReservation = conn.prepareStatement(RETRIEVE_ALL_RESERVATION);
		retrieveIdReservationPay = conn.prepareStatement(RETRIEVE_ID_RESERVATION_PAY);
		searchFlights = conn.prepareStatement(SEARCH_FLIGHTS);
		paySuccessStatement = conn.prepareStatement(PAY_SUCCESS_STATEMENT);
		updateUserBalance =conn.prepareStatement(UPDATE_USER_BALANCE);
		retrieveIdReservationCancel = conn.prepareStatement(RETRIEVE_ID_RESERVATION_CANCEL);
		beginTransactionRepeatedReadStatement = conn.prepareStatement(BEGIN_TRANSACTION_REPEATEDREAD_SQL);
		cancelSuccessStatement1 = conn.prepareStatement(CANCEL_SUCCESS_STATEMENT1);
		cancelSuccessStatement2 = conn.prepareStatement(CANCEL_SUCCESS_STATEMENT2);
		beginTransactionStatement = conn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = conn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = conn.prepareStatement(ROLLBACK_SQL);
		checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
	}

	/**
	 * Takes a user's username and password and attempts to log the user in.
	 *
	 * @param username
	 * @param password
	 *
	 * @return If someone has already logged in, then return "User already logged in\n"
	 * For all other errors, return "Login failed\n".
	 *
	 * Otherwise, return "Logged in as [username]\n".
	 */
	public String transaction_login(String username, String password)
	{
		try{
			if(this.username != null){
				return "User already logged in\n";
			}else if(username.length() == 0 || password.length() == 0){
				return "Login failed\n";
			}else{
				logInStatement.clearParameters();
				logInStatement.setString(1, username);
				logInStatement.setString(2, password);
				ResultSet userResult = logInStatement.executeQuery();
				if (userResult.next()){
					this.username = username;
					deleteItinerariesStatement.setString(1, username);
					deleteItinerariesStatement.executeUpdate();
					return "Logged in as " + username + "\n";
				}
			}
		} catch (SQLException e) { 
			e.printStackTrace(); 
		}
		return "Login failed\n";
	}

	/**
	 * Implement the create user function.
	 *
	 * @param username new user's username. User names are unique the system.
	 * @param password new user's password.
	 * @param initAmount initial amount to deposit into the user's account, should be >= 0 (failure otherwise).
	 *
	 * @return either "Created user {@code username}\n" or "Failed to create user\n" if failed.
	 * @throws SQLException 
	 */
	public String transaction_createCustomer (String username, String password, int initAmount) 
	{
		try{
			if(username.length() == 0 || password.length() ==0 || initAmount < 0){
				return "Failed to create user\n";
			}else{
				createUserStatement.clearParameters();
				createUserStatement.setString(1,username);
				createUserStatement.setString(2,password);
				createUserStatement.setInt(3,initAmount);
				createUserStatement.executeUpdate();
				return "Created user " + username + "\n";
			}
		} catch (SQLException e) { 
		}
		return "Failed to create user\n";
	}

	/**
	 * Implement the search function.
	 *
	 * Searches for flights from the given origin city to the given destination
	 * city, on the given day of the month. If {@code directFlight} is true, it only
	 * searches for direct flights, otherwise is searches for direct flights
	 * and flights with two "hops." Only searches for up to the number of
	 * itineraries given by {@code numberOfItineraries}.
	 *
	 * The results are sorted based on total flight time.
	 *
	 * @param originCity
	 * @param destinationCity
	 * @param directFlight if true, then only search for direct flights, otherwise include indirect flights as well
	 * @param dayOfMonth
	 * @param numberOfItineraries number of itineraries to return
	 *
	 * @return If no itineraries were found, return "No flights match your selection\n".
	 * If an error occurs, then return "Failed to search\n".
	 *
	 * Otherwise, the sorted itineraries printed in the following format:
	 *
	 * Itinerary [itinerary number]: [number of flights] flight(s), [total flight time] minutes\n
	 * [first flight in itinerary]\n
	 * ...
	 * [last flight in itinerary]\n
	 *
	 * Each flight should be printed using the same format as in the {@code Flight} class. Itinerary numbers
	 * in each search should always start from 0 and increase by 1.
	 * @throws SQLException 
	 *
	 * @see Flight#toString()
	 */
	public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
			int numberOfItineraries)
	{    
		try {
			beginTransaction(3);
			int index = 0;
			deleteItinerariesStatement.setString(1, this.username);
			deleteItinerariesStatement.executeUpdate();
			List<Itineraries> iTList = new ArrayList<Itineraries>();
			directFlightStatement.clearParameters();
			directFlightStatement.setInt(1, numberOfItineraries);
			directFlightStatement.setString(2, originCity);
			directFlightStatement.setString(3, destinationCity);
			directFlightStatement.setInt(4, dayOfMonth);
			ResultSet directFlightSet = directFlightStatement.executeQuery();
			while(directFlightSet.next()) {
				int fid = directFlightSet.getInt("fid");
				int time = directFlightSet.getInt("actual_time");
				int capacity = directFlightSet.getInt("capacity");
				Flight eachFlight = new Flight(fid, dayOfMonth,
						directFlightSet.getString("carrier_id"),
						directFlightSet.getInt("flight_num") + "",
						originCity, destinationCity, time, capacity,
						directFlightSet.getInt("price"));
				insertSeatStatement1.setInt(1, fid);
				insertSeatStatement1.executeUpdate();
				iTList.add(new Itineraries(time, eachFlight, null));	
				index++;
			}
			directFlightSet.close();
			if(!directFlight) {	
				twoHopStatement.clearParameters();
				twoHopStatement.setInt(1, numberOfItineraries);
				twoHopStatement.setString(2, originCity);
				twoHopStatement.setString(3, destinationCity);
				twoHopStatement.setInt(4, dayOfMonth);
				ResultSet twoHopFlightSet = twoHopStatement.executeQuery();
				while(twoHopFlightSet.next() && index < numberOfItineraries) {
					int time = twoHopFlightSet.getInt("time");
					int f1_fid = twoHopFlightSet.getInt("f1_fid");
					int f2_fid = twoHopFlightSet.getInt("f2_fid");
					Flight flight1 = new Flight(f1_fid, dayOfMonth,
							twoHopFlightSet.getString("f1_carrier"),
							twoHopFlightSet.getInt("f1_flight_num") + "",
							originCity,
							twoHopFlightSet.getString("f1_dest_city"),
							twoHopFlightSet.getInt("f1_actual_time"),
							twoHopFlightSet.getInt("f1_capacity"),
							twoHopFlightSet.getInt("f1_price"));
					Flight flight2 = new Flight(f2_fid,dayOfMonth,
							twoHopFlightSet.getString("f2_carrier"),
							twoHopFlightSet.getInt("f2_flight_num") + "",
							twoHopFlightSet.getString("f2_origin_city"),
							destinationCity,
							twoHopFlightSet.getInt("f2_actual_time"),
							twoHopFlightSet.getInt("f2_capacity"),
							twoHopFlightSet.getInt("f2_price"));
					insertSeatStatement2.setInt(1, f1_fid);
					insertSeatStatement2.setInt(2, f2_fid);
					insertSeatStatement2.executeUpdate();					
					iTList.add(new Itineraries(time, flight1, flight2));	
					index++;
				}
				twoHopFlightSet.close();
			}
			commitTransaction();
			Collections.sort(iTList, new sortItineraries());
			String printOut = printFlights(iTList, numberOfItineraries);
			return printOut;
		} catch (SQLException e) {
		}
		return "Failed to search\n";
		//return transaction_search_unsafe(originCity, destinationCity, directFlight, dayOfMonth, numberOfItineraries);
	}
	
	private String printFlights(List<Itineraries> iTList, int itinerary) {
		try {
			String printOut = "";
			int index = 0;
			beginTransaction(4);
			for(Itineraries eachItinerary : iTList) {
				Flight f1 = eachItinerary.f1;
				Flight f2 = eachItinerary.f2;
				if(f2 == null) {
					insertItinerariesTable(index, f1.fid, -1, f1.price, f1.dayOfMonth);
					printOut += "Itinerary " + index + ": 1 flight(s), " + eachItinerary.time + " minutes\n" + 
							f1.toString() + "\n";
				}else {
					insertItinerariesTable(index, f1.fid, f2.fid, f1.price + f2.price, f1.dayOfMonth);
					printOut += "Itinerary " + index + ": 2 flight(s), " + eachItinerary.time + " minutes\n" + 
							f1.toString() + "\n" + f2.toString() + "\n";
				}
				index++;
				if(index == itinerary) {
					break;
				}
			}
			commitTransaction();
			if(printOut.length() == 0) {
				return "No flights match your selection\n";
			} else {
				return printOut;
			}
		} catch (SQLException e) {		
		}
		return "";
	}

	private void insertItinerariesTable(int index, int flight1, int flight2, int price, int day) throws SQLException{
		if(this.username != null) {
			insertItinerariesStatement.setInt(1, index);
			insertItinerariesStatement.setInt(2, flight1);
			insertItinerariesStatement.setInt(3, flight2);
			insertItinerariesStatement.setInt(4, price);
			insertItinerariesStatement.setInt(5, day);
			insertItinerariesStatement.setString(6, this.username);
			insertItinerariesStatement.executeUpdate();
		}
	}

	/**
	 * Same as {@code transaction_search} except that it only performs single hop search and
	 * do it in an unsafe manner.
	 *
	 * @param originCity
	 * @param destinationCity
	 * @param directFlight
	 * @param dayOfMonth
	 * @param numberOfItineraries
	 *
	 * @return The search results. Note that this implementation *does not conform* to the format required by
	 * {@code transaction_search}.
	 */
	private String transaction_search_unsafe(String originCity, String destinationCity, boolean directFlight,
			int dayOfMonth, int numberOfItineraries)
	{
		StringBuffer sb = new StringBuffer();

		try
		{
			// one hop itineraries
			String unsafeSearchSQL =
					"SELECT TOP (" + numberOfItineraries + ") day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price "
							+ "FROM Flights "
							+ "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity + "\' AND day_of_month =  " + dayOfMonth + " "
							+ "ORDER BY actual_time ASC";

			Statement searchStatement = conn.createStatement();
			ResultSet oneHopResults = searchStatement.executeQuery(unsafeSearchSQL);

			while (oneHopResults.next())
			{
				int result_dayOfMonth = oneHopResults.getInt("day_of_month");
				String result_carrierId = oneHopResults.getString("carrier_id");
				String result_flightNum = oneHopResults.getString("flight_num");
				String result_originCity = oneHopResults.getString("origin_city");
				String result_destCity = oneHopResults.getString("dest_city");
				int result_time = oneHopResults.getInt("actual_time");
				int result_capacity = oneHopResults.getInt("capacity");
				int result_price = oneHopResults.getInt("price");

				sb.append("Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum + " Origin: " + result_originCity + " Destination: " + result_destCity + " Duration: " + result_time + " Capacity: " + result_capacity + " Price: " + result_price + "\n");
			}
			oneHopResults.close();
		} catch (SQLException e) { e.printStackTrace(); }

		return sb.toString();
	}

	/**
	 * Implements the book itinerary function.
	 *
	 * @param itineraryId ID of the itinerary to book. This must be one that is returned by search in the current session.
	 *
	 * @return If the user is not logged in, then return "Cannot book reservations, not logged in\n".
	 * If try to book an itinerary with invalid ID, then return "No such itinerary {@code itineraryId}\n".
	 * If the user already has a reservation on the same day as the one that they are trying to book now, then return
	 * "You cannot book two flights in the same day\n".
	 * For all other errors, return "Booking failed\n".
	 *
	 * And if booking succeeded, return "Booked flight(s), reservation ID: [reservationId]\n" where
	 * reservationId is a unique number in the reservation system that starts from 1 and increments by 1 each time a
	 * successful reservation is made by any user in the system.
	 * @throws SQLException 
	 */
	public String transaction_book(int itineraryId)
	{
		if (this.username == null) {
			return "Cannot book reservations, not logged in\n";
		}
		
		try {
			searchItineraries.setInt(1, itineraryId);
			searchItineraries.setString(2, this.username);
			ResultSet search = searchItineraries.executeQuery();
			if(!search.next()) {
				return "No such itinerary " + itineraryId + "\n";
			}
			beginTransaction(4);
			searchItinerariesStatement.setInt(1, itineraryId);
			searchItinerariesStatement.setString(2, this.username);
			ResultSet itinerariesResult = searchItinerariesStatement.executeQuery();
			itinerariesResult.next();
			int first_flight = itinerariesResult.getInt("f1_fid");
			int second_flight = itinerariesResult.getInt("f2_fid");
			int price = itinerariesResult.getInt("price");
			int day = itinerariesResult.getInt("day");
			searchUserBookAlready.setString(1, this.username);
			searchUserBookAlready.setInt(2, day);
			ResultSet userBookAlready = searchUserBookAlready.executeQuery();
			if(userBookAlready.next()) {
				rollbackTransaction();
				return "You cannot book two flights in the same day\n";
			}
			if(second_flight == -1) {
				insertReservations1.setString(1, this.username);
				insertReservations1.setInt(2, first_flight);
				insertReservations1.setInt(3, second_flight);
				insertReservations1.setInt(4, day);
				insertReservations1.setInt(5, price);
				insertReservations1.setInt(6, first_flight);
				int returnValue = insertReservations1.executeUpdate();
				if(returnValue == 0) {
					return "Booking failed\n";
				}
			} else {			
				insertReservations2.setString(1, this.username);
				insertReservations2.setInt(2, first_flight);
				insertReservations2.setInt(3, second_flight);
				insertReservations2.setInt(4, day);
				insertReservations2.setInt(5, price);
				insertReservations2.setInt(6, first_flight);
				insertReservations2.setInt(7, second_flight);
				int returnValue = insertReservations2.executeUpdate();
				if(returnValue == 0) {
					return "Booking failed\n";
				}
			}
			commitTransaction();
			retrieveReservationID.setString(1, this.username);
			retrieveReservationID.setInt(2, first_flight);
			retrieveReservationID.setInt(3, second_flight);
			ResultSet idSet = retrieveReservationID.executeQuery();
			idSet.next();
			int id = idSet.getInt("id");
			return "Booked flight(s), reservation ID: " + id + "\n";
		} catch (SQLException e) {	
		}
		return "Booking failed\n";
	}

	/**
	 * Implements the reservations function.
	 *
	 * @return If no user has logged in, then return "Cannot view reservations, not logged in\n"
	 * If the user has no reservations, then return "No reservations found\n"
	 * For all other errors, return "Failed to retrieve reservations\n"
	 *
	 * Otherwise return the reservations in the following format:
	 *
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * Reservation [reservation ID] paid: [true or false]:\n"
	 * [flight 1 under the reservation]
	 * [flight 2 under the reservation]
	 * ...
	 *
	 * Each flight should be printed using the same format as in the {@code Flight} class.
	 *
	 * @see Flight#toString()
	 */
	public String transaction_reservations()
	{
		try {
			if (this.username == null) {
				return "Cannot view reservations, not logged in\n";
			}
			beginTransaction(3);
			retrieveAllReservation.setString(1, this.username);
			ResultSet userReservation = retrieveAllReservation.executeQuery();
			String reservationPrint = "";
			while(userReservation.next()) {
				int reservationID = userReservation.getInt("id");
				int f1_fid = userReservation.getInt("f1_fid");
				int f2_fid = userReservation.getInt("f2_fid");
				String pay = "false";
				if(userReservation.getInt("pay") == 1)
					pay = "true";
				searchFlights.setInt(1, f1_fid);
				ResultSet f1 = searchFlights.executeQuery();
				reservationPrint += "Reservation " + reservationID+ " paid: " + pay + ":\n" + allUserReservation(f1)+"\n";
				if(f2_fid != -1) {
					searchFlights.setInt(1,f2_fid);
					ResultSet f2 = searchFlights.executeQuery();
					reservationPrint += "Reservation " + reservationID+ " paid: " + pay + ":\n" + allUserReservation(f2)+"\n";
					f2.close();
				}
				f1.close();
			}
			userReservation.close();
			if(reservationPrint.length() == 0) {
				rollbackTransaction();
				return "No reservations found\n";
			}
			commitTransaction();
			return reservationPrint;
		} catch (SQLException e) {	
		}
		return "Failed to retrieve reservations\n";
	}
	private String allUserReservation(ResultSet f) throws SQLException {
		f.next();
		Flight flight = new Flight(f.getInt("fid"), f.getInt("day_of_month"), f.getString("carrier_id"), 
								   f.getString("flight_num"), f.getString("origin_city"), f.getString("dest_city"),
								   f.getInt("actual_time"), f.getInt("capacity"), f.getInt("price"));
		return flight.toString();
	}

	/**
	 * Implements the cancel operation.
	 *
	 * @param reservationId the reservation ID to cancel
	 *
	 * @return If no user has logged in, then return "Cannot cancel reservations, not logged in\n"
	 * For all other errors, return "Failed to cancel reservation [reservationId]"
	 *
	 * If successful, return "Canceled reservation [reservationId]"
	 *
	 * Even though a reservation has been canceled, its ID should not be reused by the system.
	 */
	public String transaction_cancel(int reservationId)
	{
		try {
			if (this.username == null) {
				return "Cannot cancel reservations, not logged in\n";
			}
			beginTransaction(4);
			retrieveIdReservationCancel.setInt(1, reservationId);
			retrieveIdReservationCancel.setString(2, this.username);
			ResultSet userReservation = retrieveIdReservationCancel.executeQuery();
			if(userReservation.next()) {
				int f1_fid = userReservation.getInt("f1_fid");
				int f2_fid = userReservation.getInt("f2_fid");
				if(f2_fid == -1) {
					cancelSuccessStatement1.setInt(1, reservationId);
					cancelSuccessStatement1.setInt(2, f1_fid);
					int returnValue = cancelSuccessStatement1.executeUpdate();
					if(returnValue == 0) {
						return "Failed to cancel reservation " + reservationId + "\n";
					}
					if(userReservation.getInt("pay") == 1) {
						searchUserStatement.setString(1, this.username);
						ResultSet account = searchUserStatement.executeQuery();
						account.next();
						updateUserBalance.setInt(1, account.getInt("balance") + userReservation.getInt("price"));
						updateUserBalance.setString(2, this.username);
						updateUserBalance.executeUpdate();
						
					}
				} else {
					cancelSuccessStatement2.setInt(1, reservationId);
					cancelSuccessStatement2.setInt(2, f1_fid);
					cancelSuccessStatement2.setInt(3, f2_fid);
					int returnValue = cancelSuccessStatement2.executeUpdate();
					if(returnValue == 0) {
						return "Failed to cancel reservation " + reservationId + "\n";
					}
					if(userReservation.getInt("pay") == 1) {
						searchUserStatement.setString(1, this.username);
						ResultSet account = searchUserStatement.executeQuery();
						account.next();
						updateUserBalance.setInt(1, account.getInt("balance") + userReservation.getInt("price"));
						updateUserBalance.setString(2, this.username);
						updateUserBalance.executeUpdate();
					}
				}
				commitTransaction();	
				return "Canceled reservation " + reservationId + "\n";
			}
		} catch (SQLException e) {
		}
		return "Failed to cancel reservation " + reservationId + "\n";
	}

	/**
	 * Implements the pay function.
	 *
	 * @param reservationId the reservation to pay for.
	 *
	 * @return If no user has logged in, then return "Cannot pay, not logged in\n"
	 * If the reservation is not found / not under the logged in user's name, then return
	 * "Cannot find unpaid reservation [reservationId] under user: [username]\n"
	 * If the user does not have enough money in their account, then return
	 * "User has only [balance] in account but itinerary costs [cost]\n"
	 * For all other errors, return "Failed to pay for reservation [reservationId]\n"
	 *
	 * If successful, return "Paid reservation: [reservationId] remaining balance: [balance]\n"
	 * where [balance] is the remaining balance in the user's account.
	 * @throws SQLException 
	 */
	public String transaction_pay (int reservationId) 
	{
		try {
			if (this.username == null) {
				return "Cannot pay, not logged in\n";
			}
			beginTransaction(4);
			retrieveIdReservationPay.setInt(1, reservationId);
			retrieveIdReservationPay.setString(2, this.username);
			ResultSet userReservation = retrieveIdReservationPay.executeQuery();
			if (userReservation.next()) {
				searchUserStatement.setString(1, this.username);
				ResultSet account = searchUserStatement.executeQuery();
				account.next();
				int balance = account.getInt("balance");
				int itineraryCost = userReservation.getInt("price");
				if(balance >= itineraryCost) {
					int remaining = balance - itineraryCost;
					paySuccessStatement.setInt(1, reservationId);
					int returnValue = paySuccessStatement.executeUpdate();
					if(returnValue == 0) {
						return "Failed to pay for reservation " + reservationId + "\n";
					}
					updateUserBalance.setInt(1, remaining);
					updateUserBalance.setString(2, this.username);
					updateUserBalance.executeUpdate();
					commitTransaction();		
					return "Paid reservation: " + reservationId + " remaining balance: " + remaining + "\n";
				} else {
					rollbackTransaction();
					return "User has only " + balance + " in account but itinerary costs " + itineraryCost + "\n";
				}			
			}
			rollbackTransaction();
			return "Cannot find unpaid reservation " + reservationId + " under user: " + this.username + "\n";
		} catch (SQLException e) {
		}
		return "Failed to pay for reservation " + reservationId + "\n";
	}

	/* some utility functions below */

	public void beginTransaction(int level) throws SQLException
	{
		conn.setAutoCommit(false);
		if(level ==3)
			beginTransactionRepeatedReadStatement.executeUpdate();
		else if(level ==4)
			beginTransactionStatement.executeUpdate();
	}

	public void commitTransaction() throws SQLException
	{
		commitTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	public void rollbackTransaction() throws SQLException
	{
		rollbackTransactionStatement.executeUpdate();
		conn.setAutoCommit(true);
	}

	/**
	 * Shows an example of using PreparedStatements after setting arguments. You don't need to
	 * use this method if you don't want to.
	 */
	private int checkFlightCapacity(int fid) throws SQLException
	{
		checkFlightCapacityStatement.clearParameters();
		checkFlightCapacityStatement.setInt(1, fid);
		ResultSet results = checkFlightCapacityStatement.executeQuery();
		results.next();
		int capacity = results.getInt("capacity");
		results.close();

		return capacity;
	}
}
