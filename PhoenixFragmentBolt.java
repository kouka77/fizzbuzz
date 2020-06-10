package io.barac;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PhoenixFragmentBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	
	String REQUESTID,TENANTID,TRANSACTIONTYPE,ACCOUNTINDICATOR,TIMESTAMP,
	APPLICATIONID,DEVICETYPE,APPLICATIONTYPE,FINGERPRINT,UIDSOURCE,CUSTOMERID,
	USERNAME,ACCOUNTNUMBER,SESSIONID,UID,IP,LONGITUDE,LATITUDE,UAS,DESTINATIONACCOUNTNUMBER,
	BENEFICIARYBANKNAME,BENEFICIARYBRANCHNAME,BENEFICIARYBANKCOUNTRY,BENEFICIARYIBAN,BENEFICIARYSWIFT,
	BENEFICIARYROUTINGNUMBER,BENEFICIARYFULLNAME,BENEFICIARYNICKNAME,AMOUNT,CURRENCYTYPE,ORIGINCURRENCYAMOUNT,
	ORIGINCURRENCYTYPE,CURRENCYEXCHANGERATE,BALANCE,AUTHSTATUS,NARRATION,FINGERPRINTBUFFER;
	
	String s;

	Connection connection;
	Statement statement;
	private Fields fields;

	public PhoenixFragmentBolt() {
		fields = new Fields("requestID","tenantID","transactionType","accountIndicator","timestamp","applicationID","deviceType","applicationType","fingerPrint","uidSource","customerID","userName","accountNumber","sessionID","uid","ip","longitude","latitude","uas","destinationAccountNumber","beneficiaryBankName","beneficiaryBranchName","beneficiaryBankCountry","beneficiaryIBAN","beneficiarySWIFT","beneficiaryRoutingNumber","beneficiaryFullName","beneficiaryNickName","amount","currencyType","originCurrencyAmount","originCurrencyType","currencyExchangeRate","balance","authStatus","narration","fingerPrintBuffer");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// System.out.println(" try loading driver");
		try {
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			// below is the cnx string for production
			// connection =
			// DriverManager.getConnection("jdbc:phoenix:10.0.0.35,10.0.0.36,10.0.0.37,10.0.0.38:2181:/hbase-unsecure");

			// below is the cnx string fot Dev
			connection = DriverManager
					.getConnection("jdbc:phoenix:datanode.europe-west2-c.c.centered-accord-266509.internal:2181:/hbase-unsecure");

			statement = connection.createStatement();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		REQUESTID = input.getString(0);
		TENANTID = input.getString(1);  
		TRANSACTIONTYPE = input.getString(2);
		ACCOUNTINDICATOR = input.getString(3);
		TIMESTAMP = input.getString(4);
		APPLICATIONID = input.getString(5);
		DEVICETYPE = input.getString(6);
		APPLICATIONTYPE = input.getString(7);
		FINGERPRINT = input.getString(8);
		UIDSOURCE = input.getString(9);
		CUSTOMERID = input.getString(10);
		USERNAME = input.getString(11);
		ACCOUNTNUMBER = input.getString(12);
		SESSIONID = input.getString(13);
		UID = input.getString(14);
		IP = input.getString(15);
		LONGITUDE = input.getString(16);
		LATITUDE = input.getString(17);
		UAS = input.getString(18);
		DESTINATIONACCOUNTNUMBER = input.getString(19);
		BENEFICIARYBANKNAME = input.getString(20);
		BENEFICIARYBRANCHNAME = input.getString(21);
		BENEFICIARYBANKCOUNTRY = input.getString(22);
		BENEFICIARYIBAN = input.getString(23);
		BENEFICIARYSWIFT = input.getString(24);
		BENEFICIARYROUTINGNUMBER = input.getString(25);
		BENEFICIARYFULLNAME = input.getString(26);
		BENEFICIARYNICKNAME = input.getString(27);
		AMOUNT = input.getString(28);
		CURRENCYTYPE = input.getString(29);
		ORIGINCURRENCYAMOUNT = input.getString(30);
		ORIGINCURRENCYTYPE = input.getString(31);
		CURRENCYEXCHANGERATE = input.getString(32);
		BALANCE = input.getString(33);
		AUTHSTATUS = input.getString(34);
		NARRATION = input.getString(35);
		FINGERPRINTBUFFER = input.getString(36);
		
		
		

		String query = "upsert into Transaction(REQUESTID,TENANTID,TRANSACTIONTYPE,ACCOUNTINDICATOR,TIMESTAMP,APPLICATIONID,DEVICETYPE,APPLICATIONTYPE,FINGERPRINT,UIDSOURCE,CUSTOMERID,USERNAME,ACCOUNTNUMBER,SESSIONID,UID,IP,LONGITUDE,LATITUDE,UAS,DESTINATIONACCOUNTNUMBER,BENEFICIARYBANKNAME,BENEFICIARYBRANCHNAME,BENEFICIARYBANKCOUNTRY,BENEFICIARYIBAN,BENEFICIARYSWIFT,BENEFICIARYROUTINGNUMBER,BENEFICIARYFULLNAME,BENEFICIARYNICKNAME,AMOUNT,CURRENCYTYPE,ORIGINCURRENCYAMOUNT,ORIGINCURRENCYTYPE,CURRENCYEXCHANGERATE,BALANCE,AUTHSTATUS,NARRATION,FINGERPRINTBUFFER) values( '"
		+REQUESTID+ "','" +TENANTID+ "','" +TRANSACTIONTYPE+ "','" +ACCOUNTINDICATOR+ "','" +TIMESTAMP+ "','" +APPLICATIONID+ "','" +DEVICETYPE+ "','" +APPLICATIONTYPE+ "','" +FINGERPRINT+ "','" +UIDSOURCE+ "','" +CUSTOMERID+ "','" +USERNAME+ "','" +ACCOUNTNUMBER+ "','" +SESSIONID+ "','" +UID+ "','" +IP+ "','" +LONGITUDE+ "','" +LATITUDE+ "','" +UAS+ "','" +DESTINATIONACCOUNTNUMBER+ "','" +BENEFICIARYBANKNAME+ "','" +BENEFICIARYBRANCHNAME+ "','" +BENEFICIARYBANKCOUNTRY+ "','" +BENEFICIARYIBAN+ "','" +BENEFICIARYSWIFT+ "','" +BENEFICIARYROUTINGNUMBER+ "','" +BENEFICIARYFULLNAME+ "','" +BENEFICIARYNICKNAME+ "','" +AMOUNT+ "','" +CURRENCYTYPE+ "','" +ORIGINCURRENCYAMOUNT+ "','" +ORIGINCURRENCYTYPE+ "','" +CURRENCYEXCHANGERATE+ "','" +BALANCE+ "','" +AUTHSTATUS+ "','" +NARRATION+ "','" +FINGERPRINTBUFFER+ "')";
		try {
		    System.out.println(query);
			statement.executeUpdate(query);
			connection.commit();
			collector.emit(new Values(REQUESTID,TENANTID,TRANSACTIONTYPE,ACCOUNTINDICATOR,TIMESTAMP,APPLICATIONID,DEVICETYPE,APPLICATIONTYPE,FINGERPRINT,UIDSOURCE,CUSTOMERID,USERNAME,ACCOUNTNUMBER,SESSIONID,UID,IP,LONGITUDE,LATITUDE,UAS,DESTINATIONACCOUNTNUMBER,BENEFICIARYBANKNAME,BENEFICIARYBRANCHNAME,BENEFICIARYBANKCOUNTRY,BENEFICIARYIBAN,BENEFICIARYSWIFT,BENEFICIARYROUTINGNUMBER,BENEFICIARYFULLNAME,BENEFICIARYNICKNAME,AMOUNT,CURRENCYTYPE,ORIGINCURRENCYAMOUNT,ORIGINCURRENCYTYPE,CURRENCYEXCHANGERATE,BALANCE,AUTHSTATUS,NARRATION,FINGERPRINTBUFFER));
			

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void cleanup() {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(fields);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
