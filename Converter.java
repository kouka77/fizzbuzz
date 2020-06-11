package io.barac;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.JSONException;
import org.json.JSONObject;

public class Converter extends BaseBasicBolt {
	
	String REQUESTID,TENANTID,TRANSACTIONTYPE,ACCOUNTINDICATOR,TIMESTAMP,
	APPLICATIONID,DEVICETYPE,APPLICATIONTYPE,FINGERPRINT,UIDSOURCE,CUSTOMERID,
	USERNAME,ACCOUNTNUMBER,SESSIONID,UID,IP,LONGITUDE,LATITUDE,UAS,DESTINATIONACCOUNTNUMBER,
	BENEFICIARYBANKNAME,BENEFICIARYBRANCHNAME,BENEFICIARYBANKCOUNTRY,BENEFICIARYIBAN,BENEFICIARYSWIFT,
	BENEFICIARYROUTINGNUMBER,BENEFICIARYFULLNAME,BENEFICIARYNICKNAME,AMOUNT,CURRENCYTYPE,ORIGINCURRENCYAMOUNT,
	ORIGINCURRENCYTYPE,CURRENCYEXCHANGERATE,BALANCE,AUTHSTATUS,NARRATION,FINGERPRINTBUFFER;
	
	String s;
	Values values;
	Fields fields;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("1111111111111111111111111111111");
		super.prepare(stormConf, context);
		
	}
	

	

	
	public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
		System.out.println("333333333333333333333333333333");
		ofDeclarer.declare(new Fields("requestID","tenantID","transactionType","accountIndicator","timestamp","applicationID","deviceType","applicationType","fingerPrint","uidSource","customerID","userName","accountNumber","sessionID","uid","ip","longitude","latitude","uas","destinationAccountNumber","beneficiaryBankName","beneficiaryBranchName","beneficiaryBankCountry","beneficiaryIBAN","beneficiarySWIFT","beneficiaryRoutingNumber","beneficiaryFullName","beneficiaryNickName","amount","currencyType","originCurrencyAmount","originCurrencyType","currencyExchangeRate","balance","authStatus","narration","fingerPrintBuffer"));

		
	}


	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("22222222222222222222222222222222");
		fields = input.getFields();
		s = (input.getValueByField(fields.get(0)).toString());
		System.out.println("44444444444444444444444444");
		System.out.println(s);
		
		int pos =s.indexOf("{");
		if (pos != -1) {
			s=s.substring(pos);
		
    	JSONObject jsonObject=null;

    	try {
    	     jsonObject = new JSONObject(s);
    	}catch (JSONException err){
    	     
    	} 
    	
    	try {
    	
    		REQUESTID=jsonObject.get("requestID").toString();
    		TENANTID=jsonObject.get("tenantID").toString();
    		TRANSACTIONTYPE=jsonObject.get("transactionType").toString();
    		ACCOUNTINDICATOR=jsonObject.get("accountIndicator").toString();
    		TIMESTAMP=jsonObject.get("timestamp").toString();
    		APPLICATIONID=jsonObject.get("applicationID").toString();
    		DEVICETYPE=jsonObject.get("deviceType").toString();
    		APPLICATIONTYPE=jsonObject.get("applicationType").toString();
    		FINGERPRINT=jsonObject.get("fingerPrint").toString();
    		UIDSOURCE=jsonObject.get("uidSource").toString();
    		CUSTOMERID=jsonObject.get("customerID").toString();
    		USERNAME=jsonObject.get("userName").toString();
    		ACCOUNTNUMBER=jsonObject.get("accountNumber").toString();
    		SESSIONID=jsonObject.get("sessionID").toString();
    		UID=jsonObject.get("uid").toString();
    		IP=jsonObject.get("ip").toString();
    		LONGITUDE=jsonObject.get("longitude").toString();
    		LATITUDE=jsonObject.get("latitude").toString();
    		UAS=jsonObject.get("uas").toString();
    		DESTINATIONACCOUNTNUMBER=jsonObject.get("destinationAccountNumber").toString();
    		BENEFICIARYBANKNAME=jsonObject.get("beneficiaryBankName").toString();
    		BENEFICIARYBRANCHNAME=jsonObject.get("beneficiaryBranchName").toString();
    		BENEFICIARYBANKCOUNTRY=jsonObject.get("beneficiaryBankCountry").toString();
    		BENEFICIARYIBAN=jsonObject.get("beneficiaryIBAN").toString();
    		BENEFICIARYSWIFT=jsonObject.get("beneficiarySWIFT").toString();
    		BENEFICIARYROUTINGNUMBER=jsonObject.get("beneficiaryRoutingNumber").toString();
    		BENEFICIARYFULLNAME=jsonObject.get("beneficiaryFullName").toString();
    		BENEFICIARYNICKNAME=jsonObject.get("beneficiaryNickName").toString();
    		AMOUNT=jsonObject.get("amount").toString();
    		CURRENCYTYPE=jsonObject.get("currencyType").toString();
    		ORIGINCURRENCYAMOUNT=jsonObject.get("originCurrencyAmount").toString();
    		ORIGINCURRENCYTYPE=jsonObject.get("originCurrencyType").toString();
    		CURRENCYEXCHANGERATE=jsonObject.get("currencyExchangeRate").toString();
    		BALANCE=jsonObject.get("balance").toString();
    		AUTHSTATUS=jsonObject.get("authStatus").toString();
    		NARRATION=jsonObject.get("narration").toString();
    		FINGERPRINTBUFFER=jsonObject.get("fingerPrintBuffer").toString();

    	
    	values = new Values(REQUESTID,TENANTID,TRANSACTIONTYPE,ACCOUNTINDICATOR,TIMESTAMP,
    			APPLICATIONID,DEVICETYPE,APPLICATIONTYPE,FINGERPRINT,UIDSOURCE,CUSTOMERID,
    			USERNAME,ACCOUNTNUMBER,SESSIONID,UID,IP,LONGITUDE,LATITUDE,UAS,DESTINATIONACCOUNTNUMBER,
    			BENEFICIARYBANKNAME,BENEFICIARYBRANCHNAME,BENEFICIARYBANKCOUNTRY,BENEFICIARYIBAN,BENEFICIARYSWIFT,
    			BENEFICIARYROUTINGNUMBER,BENEFICIARYFULLNAME,BENEFICIARYNICKNAME,AMOUNT,CURRENCYTYPE,ORIGINCURRENCYAMOUNT,
    			ORIGINCURRENCYTYPE,CURRENCYEXCHANGERATE,BALANCE,AUTHSTATUS,NARRATION,FINGERPRINTBUFFER);
    	
    	
    	System.out.println("Emitted values: " + values);
		collector.emit(values);
		System.out.println("Data is : " +REQUESTID+TENANTID+TRANSACTIONTYPE+ACCOUNTINDICATOR+TIMESTAMP+
				APPLICATIONID+DEVICETYPE+APPLICATIONTYPE+FINGERPRINT+UIDSOURCE+CUSTOMERID+
				USERNAME+ACCOUNTNUMBER+SESSIONID+UID+IP+LONGITUDE+LATITUDE+UAS+DESTINATIONACCOUNTNUMBER+
				BENEFICIARYBANKNAME+BENEFICIARYBRANCHNAME+BENEFICIARYBANKCOUNTRY+BENEFICIARYIBAN+BENEFICIARYSWIFT+
				BENEFICIARYROUTINGNUMBER+BENEFICIARYFULLNAME+BENEFICIARYNICKNAME+AMOUNT+CURRENCYTYPE+ORIGINCURRENCYAMOUNT+
				ORIGINCURRENCYTYPE+CURRENCYEXCHANGERATE+BALANCE+AUTHSTATUS+NARRATION+FINGERPRINTBUFFER);
		
    	}catch (JSONException err){
   	     
    	} 
		}
				

		
	}



}
