package com.zemanta.pysandra; 
import java.io.IOException;
import java.io.StringWriter;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


public class JsonRpcResponse {
	
	public static final String STATUS_OK = "ok";
	public static final String STATUS_ERROR = "error";
	
	private String status;
	private String statusMsg;
	
	public JsonRpcResponse(String statusMsg, String status) {
		this.statusMsg = statusMsg;
		this.status = status;
	}

	public JsonRpcResponse(String jstring) throws JsonRpcResponseException {
		try {
			JSONObject obj = (JSONObject) JSONValue.parse(jstring);
			status = (String) obj.get("status");
			statusMsg = (String) obj.get("statusMsg");
		} catch (Exception e) {
			throw new JsonRpcResponseException("Couldn't parse response json: " + e);
		}
		
		if (status == null) {
			throw new JsonRpcResponseException("Missing field status");
		}
		if (statusMsg == null) {
			throw new JsonRpcResponseException("Missing field statusMsg");
		}
	}
	
	@SuppressWarnings("unchecked")
	public String toJSON() throws IOException {
		JSONObject obj = new JSONObject();
		obj.put("status", this.status);
		obj.put("statusMsg", this.statusMsg);

		StringWriter out = new StringWriter();
		obj.writeJSONString(out);
		return out.toString();
	}
	
	public String getStatus() {
		return status;
	}
	
	public boolean isStatusOk() {
		return status.equals(STATUS_OK);
	}
	
	public String getStatusMsg() {
		return statusMsg;
	}
	
	public boolean isLast() {
		return false;
	}
	
	
}

class JsonRpcOkResponse extends JsonRpcResponse {

	public JsonRpcOkResponse(String response) {
		super(response, STATUS_OK);
	}
	
	public JsonRpcOkResponse() {
		super("", STATUS_OK);
	}
	
}

class JsonRpcOkStopResponse extends JsonRpcOkResponse {
	public boolean isLast() {
		return true;
	}
	
}

class JsonRpcErrorResponse extends JsonRpcResponse {

	public JsonRpcErrorResponse(String response) {
		super(response, STATUS_ERROR);
	}
	
}

class JsonRpcResponseException extends Exception {
	public JsonRpcResponseException(String string) {
		super(string);
	}

	private static final long serialVersionUID = 1L;
}




