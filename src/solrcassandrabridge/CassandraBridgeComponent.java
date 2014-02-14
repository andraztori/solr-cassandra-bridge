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

package solrcassandrabridge;

import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.SolrParams;



import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.model.AllOneConsistencyLevelPolicy;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

import me.prettyprint.cassandra.serializers.BigIntegerSerializer;



import java.util.Iterator;

import org.apache.solr.update.UpdateLog;
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
/**
 * TODO!
 *
 *
 * @since solr 1.3
 */
public class CassandraBridgeComponent extends SearchComponent implements PluginInfoInitialized, SolrCoreAware
{
	public static final String COMPONENT_NAME = "solrcassandrabridge";
	private PluginInfo info = PluginInfo.EMPTY_INFO;
	public static Logger log = LoggerFactory.getLogger(CassandraBridgeComponent.class);
	
	private static final StringSerializer stringSerializer = StringSerializer.get();
	private static final BigIntegerSerializer bigIntegerSerializer = BigIntegerSerializer.get();
	
	SolrParams params;
	
	@Override
	public void init(PluginInfo info) {
		this.info = info;
		setupCassandra(SolrParams.toSolrParams(info.initArgs));
		log.warn("A");
		log.info("xxxB");
		
	}

	@Override
	public void prepare(ResponseBuilder rb) throws IOException {
		log.warn("ACC");
		log.info("BDDd");
		params = rb.req.getParams();
	}

	@Override
	public void inform(SolrCore core) {
		log.warn("A2");
		log.info("B2");
	}


	public void getFieldsFromCassandra(List<BigInteger> docid_list, HashMap<BigInteger, HashMap<String, String>> output_map) {
		MultigetSliceQuery<BigInteger, String, String> multigetSliceQuery = HFactory.createMultigetSliceQuery(cassandra_keyspace, bigIntegerSerializer, stringSerializer, stringSerializer);
		multigetSliceQuery.setColumnFamily(cassandra_column_family_name);
		multigetSliceQuery.setColumnNames("title", "body");
		
		// Fetch retweets data from Cassandra
		long cassandra_start_time = System.currentTimeMillis();
		multigetSliceQuery.setKeys(docid_list);
		
		QueryResult<Rows<BigInteger, String, String>> result = null;
		try {
			result = multigetSliceQuery.execute();
		} catch(Exception e) {
			log.warn("Error while executing Cassandra query.", e);
			return;
		}

		Rows<BigInteger, String, String> result_rows = result.get();
		// turn result into a map
		for (Row<BigInteger, String, String> row : result_rows) {
			BigInteger key = row.getKey();
			ColumnSlice<String, String> column_slice = row.getColumnSlice();
			HColumn<String, String> title_column = column_slice.getColumnByName("title");
			HColumn<String, String> text_column = column_slice.getColumnByName("body");
			if (title_column != null)
			{
				log.info(title_column.getValue());
				output_map.get(key).put("title", title_column.getValue());
			}
			if (text_column != null)
			{
				output_map.get(key).put("body", text_column.getValue());
				log.info(text_column.getValue());
			}
		}
		
		long cassandra_end_time = System.currentTimeMillis();
		log.info("Requested " + docid_list.size() + " documents from Cassandra. The request took " + (cassandra_end_time - cassandra_start_time) + " miliseconds.");

	}

	@Override
	public void process(ResponseBuilder rb) throws IOException {
			
		log.info("process()");
		Set<String> fields = new HashSet<String>();
		fields.add("id");
		SolrDocumentList docs = SolrPluginUtils.docListToSolrDocumentList(
			rb.getResults().docList, 
			rb.req.getSearcher(), 
			fields,
			null );

		List<BigInteger> docid_list = new ArrayList<BigInteger>();

		HashMap<BigInteger, HashMap<String, String>> output_map = new HashMap<BigInteger, HashMap<String, String>>();

		for( SolrDocument doc : docs ) {
			int docid = (int)doc.getFieldValue("id");
			log.info(String.valueOf(docid));
			docid_list.add(BigInteger.valueOf(docid));
			// prepare an output map
			output_map.put(BigInteger.valueOf(docid), new HashMap<String, String>());
		}

		getFieldsFromCassandra(docid_list, output_map);

		for( SolrDocument doc : docs ) {
			int docid = (int)doc.getFieldValue("id");
			log.info(String.valueOf(docid));
			for (Map.Entry<String, String> entry : output_map.get(BigInteger.valueOf(docid)).entrySet()){
				doc.setField(entry.getKey(), entry.getValue());
			}
		}		  

		NamedList vals = rb.rsp.getValues();
		int idx = vals.indexOf( "response", 0 );
		if( idx >= 0 ) {
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



	private Cluster cassandra_cluster;
	private Keyspace cassandra_keyspace;
	private String cassandra_column_family_name;
	
	private boolean setupCassandra(SolrParams params) {
		String cassandra_cluster_name = params.get("cassandra_cluster_name");
		log.info(cassandra_cluster_name);
		String cassandra_servers = params.get("cassandra_servers");
		String cassandra_keyspace_name = params.get("cassandra_keyspace");
		cassandra_column_family_name = params.get("cassandra_column_family");
		
		if(cassandra_cluster_name == null || cassandra_servers == null || cassandra_keyspace_name == null || cassandra_column_family_name == null){
			log.warn("Will not fetch additional documents due to `cassandra_cluster_name` and/or `cassandra_servers` parameters are not set!");
			return false;
		}
		
		cassandra_cluster = HFactory.getOrCreateCluster(cassandra_cluster_name, new CassandraHostConfigurator(cassandra_servers));
		cassandra_keyspace = HFactory.createKeyspace(cassandra_keyspace_name, cassandra_cluster);
		cassandra_keyspace.setConsistencyLevelPolicy(new AllOneConsistencyLevelPolicy());
		
		return true;
	}


















}
