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
		

	static public Process process;
	
	private JsonRpcResponse sendRequestGetResponse(JsonRpcRequest request) {
		try {
			String serialized = request.toJSON() + "\n";
			log.debug("Request:" + serialized);
			out_stream.write(serialized.getBytes());
			out_stream.flush();
			String response_str = in_reader.readLine();
			log.debug("Response:" + response_str);
			return new JsonRpcResponse(response_str);
		} catch (Exception e) {
			return new JsonRpcErrorResponse(e.toString());
		}
	}
	
	private void executeCommand(String command, JSONObject obj) throws Exception {
		JsonRpcRequest request = new JsonRpcRequest(command, obj);
		JsonRpcResponse response = sendRequestGetResponse(request);
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
		//Path tmpdir = Files.createTempDirectory("pysandra");
		String tmpdir = "target";
		log.debug("Using temporary directory: " + tmpdir);
		JSONObject obj = new JSONObject();
		obj.put("tmpdir", tmpdir);
		String current = new java.io.File( "." ).getCanonicalPath();
		log.debug("path: " + current);
		obj.put("yamlconf", current + "/src/test/resources/cu-cassandra.yaml");
		executeCommand("start", obj);
	}	

	
	public void stop_process() throws Exception {
		log.debug("Command 'stop' starting");
		JSONObject obj = new JSONObject();
		executeCommand("stop", obj);
		log.debug("Command 'stop' returned, waiting for process termination");
		process.waitFor();
		log.debug("Command 'stop' finished");
		
	}
	

	public void load_data_cql(String fileName) throws Exception {
		/* We only support CQL since this is the only thing that reliably worked in cassandra-unit */
		log.debug("Command 'load' starting");
		log.debug("Filename: " + fileName);
		JSONObject obj = new JSONObject();
		obj.put("filename", fileName);
		obj.put("type", "cql");
		obj.put("host", "localhost");
		obj.put("rpc_port", 9171);
		obj.put("native_transport_port", 9142);
		
		executeCommand("load", obj);
		log.debug("Command 'load' finished");
		
	}


	public void clean_data() throws Exception {
		log.debug("Command 'clean' starting");
		JSONObject obj = new JSONObject();		
		executeCommand("clean", obj);
		log.debug("Command 'clean' finished");
		
	}
}
