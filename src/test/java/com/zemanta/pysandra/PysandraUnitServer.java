package com.zemanta.pysandra;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class PysandraUnitServer {
		
	private static JsonRpcResponse executeCommand(JsonRpcRequest request) {
		try {
			return (JsonRpcResponse) CassandraProcedures.run(request.getCommand(), request.getParam());
		} catch (Exception e) {
			return new JsonRpcErrorResponse(e.toString());
		}
	}
	

	public static void send(JsonRpcResponse response) throws IOException {
		System.out.println(response.toJSON());
	}

	
	private static JsonRpcResponse parseRequest(String jstring) throws IOException {
		JsonRpcResponse response = null;
		try {
			JsonRpcRequest request = new JsonRpcRequest(jstring);
			response = executeCommand(request);
		} catch (JsonRpcRequestException e) {
			response = new JsonRpcErrorResponse(e.toString());
		}
		return response;
	}


	
	private static void inputReader() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
		while((input=br.readLine()) != null) {
			JsonRpcResponse response = parseRequest(input);
			send(response);
			if (response.isLast()) {
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
