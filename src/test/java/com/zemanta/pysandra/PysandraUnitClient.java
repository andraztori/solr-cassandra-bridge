package com.zemanta.pysandra;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class PysandraUnitClient {

	public static Logger log = LoggerFactory.getLogger(PysandraUnitClient.class);
	OutputStream out_stream;
	BufferedReader in_reader;

	public PysandraUnitClient() {
	}
		

	static Process process;
	
	private JsonRpcResponse executeCommand(JsonRpcRequest request) {
		try {
			String serialized = request.toJSON() + "\n";
			log.debug("Request:" + serialized);
			out_stream.write(serialized.getBytes());
			out_stream.flush();
			String response_str = in_reader.readLine();
			log.debug("Response:" + response_str);
		//	response_str = "{\"status\":\"ok\",\"statusMsg\":\"\"}";
			JsonRpcResponse r = new JsonRpcResponse(response_str);
			return r;
		} catch (Exception e) {
			return new JsonRpcErrorResponse(e.toString());
		}
	}
	
	private void send_command(String command, JSONObject obj) throws Exception {
		JsonRpcRequest request = new JsonRpcRequest(command, obj);
		JsonRpcResponse response = executeCommand(request);
		if (!response.isStatusOk()) {
			log.error("Status not ok: " + response.getStatusMsg());
			throw new Exception("Response status not ok");
		}		
	}
	
	public void start_process() throws Exception {
		String separator = System.getProperty("file.separator");
		String classpath = System.getProperty("java.class.path");
		String java_jre_path = System.getProperty("java.home")
			+ separator + "bin" + separator + "java";
		String process_name = PysandraUnitServer.class.getName();
		ProcessBuilder processBuilder = 
			new ProcessBuilder(java_jre_path, "-cp", 
			classpath, 
			process_name);
		log.debug("Starting pysandra process " + java_jre_path + " name: " + process_name);
		process = processBuilder.start();
		in_reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		out_stream = process.getOutputStream();
		log.debug("PysandraUnitServer process initiated, now sending 'start' command");
		start_command();
	}
	

	private void start_command() throws Exception {
		log.debug("Command 'start'");
		Path tmpdir = Files.createTempDirectory("pysandra");
		log.debug("Using temporary directory: " + tmpdir.toString());
		JSONObject obj = new JSONObject();
//		obj.put("tmpdir", tmpdir.toString());
		obj.put("tmpdir", "/tmp/x");
		String current = new java.io.File( "." ).getCanonicalPath();
		log.debug("path: " + current);
//		obj.put("yamlconf", "/home/minmax/zgit/solr-cassandra-bridge/src/test/resources/cu-cassandra.yaml");
		obj.put("yamlconf", current + "/src/test/resources/cu-cassandra.yaml");
		send_command("start", obj);
	}	

	
	public void stop_process() throws Exception {
		log.debug("Command 'stop'");
		JSONObject obj = new JSONObject();
		send_command("stop", obj);
		log.debug("Command 'stop' returned, waiting for process termination");
		process.waitFor();
		log.debug("Pysandra process terminated");
		
	}
	

	public void load_data(String fileName, String type) throws Exception {
		log.debug("Command 'load'");
		log.debug("Filename: " + fileName +", type: " + type);
		JSONObject obj = new JSONObject();
		obj.put("filename", fileName);
		obj.put("type", type);
		obj.put("host", "localhost");
		obj.put("rpc_port", 9171);
		obj.put("native_transport_port", 9142);
		
		send_command("load", obj);
		log.debug("Command 'load' finished");
		
	}
	
}
