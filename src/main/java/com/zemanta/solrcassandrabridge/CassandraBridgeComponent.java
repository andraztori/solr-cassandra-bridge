/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zemanta.solrcassandrabridge;

import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;





import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.BigIntegerSerializer;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.ArrayList;
import java.math.BigInteger;

public class CassandraBridgeComponent extends SearchComponent implements PluginInfoInitialized, SolrCoreAware
{
	public static final String COMPONENT_NAME = "solrcassandrabridge";
	private PluginInfo info = PluginInfo.EMPTY_INFO;
	public static Logger log = LoggerFactory.getLogger(CassandraBridgeComponent.class);
	
	private String key_field_name;				// from solrconfig.xml  -- which key will be used to map between solr and cassandra
	private HashSet<String> bridged_fields;			// from solrconfig.xml 	-- which fields are allowed
	
	CassandraConnector cassandraConnector;			// Here we keep the connection to cassandra and auxilary functions
	
	@SuppressWarnings("unchecked")
	@Override
	public void init(PluginInfo info) {
		this.info = info;
		
		// Parse necessary parameters from solrconfig.xml section
		SolrParams params = SolrParams.toSolrParams(info.initArgs);
		bridged_fields = new HashSet<String>(((NamedList<String>)info.initArgs.get("bridged_fields")).getAll("name"));
		key_field_name = params.get("key_field_name");

		log.info("bridged_fields: " + String.valueOf(bridged_fields));
		log.info("key_field_name: " + key_field_name);
		cassandraConnector = this.new CassandraConnector();
		// Start cassandra connection, some parameters from solrconfig.xml are used
		cassandraConnector.setup(params);

	}
	
	@Override
	public void inform(SolrCore core) {
		log.warn("A2");
		log.info("B2");
		core.addCloseHook(new CloseHook() {
			@Override
			public void preClose(SolrCore core) {
				cassandraConnector.close();
			}
			@Override
			public void postClose(SolrCore core) {
			}
      		});

		
	}

	@Override
	public void prepare(ResponseBuilder rb) throws IOException {
		log.warn("ACC");
		log.info("BDDd");
	}


	@Override
	public void process(ResponseBuilder rb) throws IOException {
		
		// First we need to get Documents, so we get the "id" of the field
		Set<String> fields = new HashSet<String>();
		fields.add(key_field_name);
		SolrDocumentList docs = SolrPluginUtils.docListToSolrDocumentList(rb.getResults().docList, rb.req.getSearcher(), fields, null );

		// Docid_list is an array of ids to be retrieved
		List<BigInteger> docid_list = new ArrayList<BigInteger>();
		// We'll be putting things into output map in the form of {id: {field_name: value, ...}, ...}
		HashMap<BigInteger, HashMap<String, String>> output_map = new HashMap<BigInteger, HashMap<String, String>>();

		// Iterate through documents and get values of their id field
		for( SolrDocument doc : docs ) {
			int docid = (Integer)doc.getFieldValue(key_field_name);
			docid_list.add(BigInteger.valueOf(docid));
			// prepare an output map for this id - empty hashmaps to be filled
			output_map.put(BigInteger.valueOf(docid), new HashMap<String, String>());
		}
				
		// Intersection of requested fields and bridged fields is what we will ask cassandra for
		ReturnFields returnFields = new SolrReturnFields(rb.req.getParams().getParams(CommonParams.FL), rb.req );
		Set<String> cassandra_fields;
		if (returnFields.wantsAllFields()) {
			cassandra_fields = bridged_fields;
		} else {
			cassandra_fields = returnFields.getLuceneFieldNames();
			cassandra_fields.retainAll(bridged_fields);
		}
		log.warn("Fields." + String.valueOf(cassandra_fields));
		
		// Get specific fields from cassandra to output_map
		cassandraConnector.getFieldsFromCassandra(docid_list, output_map, new ArrayList<String>(cassandra_fields));

		// Iterate through documents for the second time
		// Add the fields that cassandra returned
		// We could skip intermediate map, but we prefer separation of code messing with cassandra from code messing with solr structures
		for( SolrDocument doc : docs ) {
			int docid = (Integer)doc.getFieldValue(key_field_name);
			for (Map.Entry<String, String> entry : output_map.get(BigInteger.valueOf(docid)).entrySet()){
				doc.setField(entry.getKey(), entry.getValue());
			}	
		}		  

		/// We replace the current response
		@SuppressWarnings("unchecked")
		NamedList<SolrDocumentList> vals = rb.rsp.getValues();
		int idx = vals.indexOf( "response", 0 );
		if( idx >= 0 ) {
			// I am pretty sure we always take this code path
			log.debug("Replacing DocList with SolrDocumentList " + docs.size());
			vals.setVal( idx, docs );
		}
		else {
			log.debug("Adding SolrDocumentList response" + docs.size());
			vals.add( "response", docs );	
		}


	}

	@Override
	public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
		log.warn("A4");
		log.info("B4");
	}

	@Override
	public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
		log.warn("A5");
		log.info("B5");
	}

	@Override
	public void finishStage(ResponseBuilder rb) {
		log.warn("A6");
		log.info("B6");
		
	}


	// Java does not allow static declarations in subclasses, so we declare then here
	private static final StringSerializer stringSerializer = StringSerializer.get();
	private static final BigIntegerSerializer bigIntegerSerializer = BigIntegerSerializer.get();
	
	// Class dealing with cassandra
	class CassandraConnector {
		
		private Cluster cassandra_cluster;
		private Keyspace cassandra_keyspace;
		private String cassandra_column_family_name;
	
		public CassandraConnector() {
		}
	
		private boolean setup(SolrParams params) {
			// Get variables from solrconfig.xml
			
			String cassandra_cluster_name = params.get("cassandra_cluster_name");
			String cassandra_servers = params.get("cassandra_servers");
			String cassandra_keyspace_name = params.get("cassandra_keyspace");
			cassandra_column_family_name = params.get("cassandra_column_family");
			
			if(cassandra_cluster_name == null || cassandra_servers == null || cassandra_keyspace_name == null || cassandra_column_family_name == null){
				log.error("Will not fetch additional documents due to `cassandra_cluster_name`, `cassandra_servers`, `cassandra_keyspace_name` or `cassandra_column_family_name` parameters not being set!");
				return false;
				// We should totally fail here, not just return false
			} else {
				log.info("Initializing connections to cassandra cluster");
			}
			
			log.info("cassandra_servers: " + String.valueOf(cassandra_servers));
			cassandra_cluster = HFactory.getOrCreateCluster(cassandra_cluster_name, new CassandraHostConfigurator(cassandra_servers));
			cassandra_keyspace = HFactory.createKeyspace(cassandra_keyspace_name, cassandra_cluster);
			cassandra_keyspace.setConsistencyLevelPolicy(new AllOneConsistencyLevelPolicy());
			log.info("Cassandra cluster connections established");
			return true;
		}
		
		public void getFieldsFromCassandra(List<BigInteger> docid_list, HashMap<BigInteger, HashMap<String, String>> output_map, List<String> fields) {
			MultigetSliceQuery<BigInteger, String, String> multigetSliceQuery = HFactory.createMultigetSliceQuery(cassandra_keyspace, bigIntegerSerializer, stringSerializer, stringSerializer);
			multigetSliceQuery.setColumnFamily(cassandra_column_family_name);
			multigetSliceQuery.setColumnNames(fields.toArray(new String[fields.size()]));
			log.info("docidlist " + docid_list.toString());
			log.info("fields " + fields.toString());
			long cassandra_start_time = System.currentTimeMillis();
			
			// Fetch data from Cassandra
			multigetSliceQuery.setKeys(docid_list);
			
			QueryResult<Rows<BigInteger, String, String>> result = null;
			try {
				result = multigetSliceQuery.execute();
			} catch(Exception e) {
				log.error("Error while executing Cassandra query.", e);
				return;
			}

			// turn result into a double map {id : {field_name: value, ...}, ...}
			for (Row<BigInteger, String, String> row : result.get()) {
				BigInteger key = row.getKey();
				log.info("aaaaaaaaaaaaaaa" + key.toString());
				List<HColumn<String, String>> column_slice = row.getColumnSlice().getColumns();
				for (HColumn<String, String> column: column_slice) {
					String field_name = column.getName();
					String field_value = column.getValue();
					log.info("got pair" + field_name + "    " + field_value);
					if (field_value != null)
					{
						log.info("got pair" + field_name + "    " + field_value);
						output_map.get(key).put(field_name, field_value);
					}
				}
			}
			
			long cassandra_end_time = System.currentTimeMillis();
			log.info("Requested " + docid_list.size() + " documents from Cassandra. The request took " + (cassandra_end_time - cassandra_start_time) + " miliseconds.");
		}

		public void close() {
			if (cassandra_cluster != null) {
				cassandra_cluster.getConnectionManager().shutdown();
			}
		}

	}
	
	////////////////////////////////////////////
	///	SolrInfoMBean
	////////////////////////////////////////////
	
	@Override
	public String getDescription() {
		return "SorlCassandraBridge";
	}
	
	@Override
	public String getSource() {
		return "$URL: https://github.com/andraztori/solr-cassandra-bridge $";
	}
	
	@Override
	public URL[] getDocs() {
		return null;
	}

}
