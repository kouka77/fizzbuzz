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
		ofDeclarer.declare(new Fields("time", "action"));

		
	}


	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("22222222222222222222222222222222");
		fields = input.getFields();
		s = (input.getValueByField(fields.get(0)).toString());
		System.out.println("44444444444444444444444444");
		System.out.println(s);
		
    	JSONObject jsonObject=null;

    	try {
    	     jsonObject = new JSONObject(s);
    	}catch (JSONException err){
    	     
    	} 
    	System.out.println("time");
    	String time=jsonObject.get("time").toString();
    	System.out.println(time);
		
    	System.out.println("action");
    	String action=jsonObject.get("action").toString();
    	System.out.println(action);

    	
    	values = new Values(time,action);
    	
    	
    	System.out.println("Emitted values: " + values);
		collector.emit(values);
		System.out.println("Data is : " +time+action);
				
		/*String url = input.getStringByField("url");
		Integer userId = input.getIntegerByField("userId");
		totalVisitCount += 1;
		outputCollector.emit(new Values(url, userId));
		outputCollector.ack(input);*/
		
	}



}
