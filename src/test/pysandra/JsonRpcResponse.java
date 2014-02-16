package pysandra; 
import java.io.IOException;
import java.io.StringWriter;

import org.json.simple.JSONObject;


abstract public class JsonRpcResponse {
	
	public final String STATUS_OK = "ok";
	public final String STATUS_ERROR = "error";
	
	protected String status;
	private String response;
	
	public JsonRpcResponse(String response) {
		this.response = response;
	}
	
	@SuppressWarnings("unchecked")
	public String toJSON() throws IOException {
		JSONObject obj = new JSONObject();
		obj.put("status", this.status);
		obj.put("value", this.response);

		StringWriter out = new StringWriter();
		obj.writeJSONString(out);
		return out.toString();
	}
	
	public boolean send() throws IOException {
		System.out.println(toJSON());
		
		return true;
	}
}

class JsonRpcOkResponse extends JsonRpcResponse {

	public JsonRpcOkResponse(String response) {
		super(response);
		this.status = this.STATUS_OK;
	}
	
	public JsonRpcOkResponse() {
		super("");
		this.status = this.STATUS_OK;
	}
	
}

class JsonRpcOkStopResponse extends JsonRpcOkResponse {

	public boolean send() throws IOException {
		super.send();
		return false;
	}
	
}

class JsonRpcErrorResponse extends JsonRpcResponse {

	public JsonRpcErrorResponse(String response) {
		super(response);
		this.status = this.STATUS_ERROR;
	}
	
}