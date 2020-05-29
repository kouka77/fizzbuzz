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

public class Converter2 extends BaseBasicBolt {
	
	String s,time,action;
    
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("1111111111111111111111111111111");
		super.prepare(stormConf, context);
		
	}
	

	

	
	public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
		System.out.println("333333333333333333333333333333");

		
	}


	public void execute(Tuple input, BasicOutputCollector collector) {
		time = input.getString(0);
		action = input.getString(1);
		System.out.println("fin is "+time+action);
				

		
	}



}