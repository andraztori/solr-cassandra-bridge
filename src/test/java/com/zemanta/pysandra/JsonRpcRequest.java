package com.zemanta.pysandra;
import java.io.IOException;
import java.io.StringWriter;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class JsonRpcRequest {

	private String command;
	private JSONObject param;
	
	public JsonRpcRequest(String jstring) throws JsonRpcRequestException {
		try {
			JSONObject obj = (JSONObject) JSONValue.parse(jstring);
			command = (String) obj.get("command");
			param = (JSONObject) obj.get("param");
		} catch (Exception e) {
			throw new JsonRpcRequestException("Couldn't parse request json: " + e);
		}
		
		if (command == null) {
			throw new JsonRpcRequestException("Missing field command");
		}
		if (param == null) {
			throw new JsonRpcRequestException("Missing field param");
		}
	}

	public JsonRpcRequest(String command_in, JSONObject param_in) {
		command = command_in;
		param = param_in;
	}

	public JSONObject getParam() {
		return param;
	}
	
	public String getCommand() {
		return command;
	}
	
	@SuppressWarnings("unchecked")
	public String toJSON() throws IOException {
		JSONObject obj = new JSONObject();
		obj.put("command", this.command);
		obj.put("param", this.param);
		StringWriter out = new StringWriter();
		obj.writeJSONString(out);
		return out.toString();
	}
	
	
}

class JsonRpcRequestException extends Exception {
	public JsonRpcRequestException(String string) {
		super(string);
	}

	private static final long serialVersionUID = 1L;
}