package pysandra;
import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.cql.FileCQLDataSet;
import org.cassandraunit.dataset.json.FileJsonDataSet;
import org.cassandraunit.dataset.xml.FileXmlDataSet;
import org.cassandraunit.dataset.yaml.FileYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.json.simple.JSONObject;


public class CassandraProcedures {
	public static List<String> validProcedures = Arrays.asList(new String[] {"start", "stop", "clean", "load"});
	
	private static boolean hasMethod(String methodName) {
		return validProcedures.contains(methodName);
	}
	
	public static Object run(String methodName, JSONObject arg) throws CassandraProceduresException {
		if (!hasMethod(methodName)) {
			throw new CassandraProceduresException("Method doesn't exist: " + methodName);
		}
		
		try {
			Method method = CassandraProcedures.class.getMethod(methodName, JSONObject.class);
			return (JsonRpcResponse) method.invoke(CassandraProcedures.class, arg);
		} catch (SecurityException e) {
		} catch (NoSuchMethodException e) {
		} catch (Exception e) {
			throw new CassandraProceduresException("Runtime error: " + e.getCause());
		}
		
		throw new CassandraProceduresException("Method doesn't exist: " + methodName);
	}
	
	public static JsonRpcResponse start(JSONObject val) {
		String tmpDir = (String)val.get("tmpdir");
		if (tmpDir == null) {
			return new JsonRpcErrorResponse("cassandra_start_error: Missing tmpdir");
		}
		
		String yamlConf = (String)val.get("yamlconf");
		if (yamlConf == null) {
			return new JsonRpcErrorResponse("cassandra_start_error: Missing yamlconf");	
		}
		
		try {
			File file = new File(yamlConf);
			EmbeddedCassandraServerHelper.startEmbeddedCassandra(file, tmpDir);
			return new JsonRpcOkResponse();
		} catch (Exception ex) {
			return new JsonRpcErrorResponse("cassandra_start_error " + ex);
		}
	}
	
	public static JsonRpcResponse stop(JSONObject val) {
		return new JsonRpcOkStopResponse();
	}
	
	public static JsonRpcResponse load(JSONObject val) {
		String fileName = (String)val.get("filename");
		
		String type = (String)val.get("type");
		String host = (String)val.get("host");
		int rpcPort = (int)(long)(Long)val.get("rpc_port");
		int nativeTransportPort = (int)(long)(Long)val.get("native_transport_port");
		
		if (host == null || fileName == null || type == null || rpcPort == 0 || nativeTransportPort == 0) {
			return new JsonRpcErrorResponse("load_data_error: Missing attribute");
		}
		
		String rpcHostPort = host + ":" + rpcPort;
		
		try {
			if (type.equals("yaml")) {
				new DataLoader("TestCluster", rpcHostPort).load(new FileYamlDataSet(fileName));
			} else if (type.equals("xml")) {
				new DataLoader("TestCluster", rpcHostPort).load(new FileXmlDataSet(fileName));
			} else if (type.equals("json")) {
				new DataLoader("TestCluster", rpcHostPort).load(new FileJsonDataSet(fileName));
			} else if (type.equals("cql")) {
				new CQLDataLoader(host, nativeTransportPort).load(new FileCQLDataSet(fileName));
			} else {
				return new JsonRpcErrorResponse("load_data_error: Invalid dataset type.");
			}
			
			return new JsonRpcOkResponse();
		} catch (Exception ex) {
			return new JsonRpcErrorResponse("load_data_error: " + ex);
		}
	}
	
	public static JsonRpcResponse clean(JSONObject val) {
		try {
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
			return new JsonRpcOkResponse();
		} catch (Exception ex) {
			return new JsonRpcErrorResponse("clean_data_error " + ex);
		}
	}
}

class CassandraProceduresException extends Exception {

	public CassandraProceduresException(String string) {
		super(string);
	}

	private static final long serialVersionUID = 1L;
	
}
