package pysandra;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class PysandraUnit {
		
	private static JsonRpcResponse executeCommand(JsonRpcRequest request) {
		try {
			return (JsonRpcResponse) CassandraProcedures.run(request.getCommand(), request.getParam());
		} catch (Exception e) {
			return new JsonRpcErrorResponse(e.toString());
		}
	}
	
	private static boolean parseRequest(String jstring) throws IOException {
		JsonRpcResponse response = null;
		try {
			JsonRpcRequest request = new JsonRpcRequest(jstring);
			response = executeCommand(request);
		} catch (JsonRpcRequestException e) {
			response = new JsonRpcErrorResponse(e.toString());
		}
		return response.send();
	}

	private static void inputReader() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
		while((input=br.readLine()) != null) {
			if (!parseRequest(input)) {
				break;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Logger.getRootLogger().setLevel(Level.OFF);
		
		inputReader();
		
		System.exit(0); // Without calling exit Cassandra daemon doesnt't exit
	}
}
