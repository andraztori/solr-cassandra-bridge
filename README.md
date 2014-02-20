solr-cassandra-bridge
=====================

Solr request handler that uses Cassandra when returning stored fields.

Basically all the Solr/SolrCloud performance advice tells you that if you need to access to full fields you should not keep them in Solr, but instead in some external document store.

The problem is that there are no simple ready-made solutions to do so. Only complex ones like DataStax Enterprise Search.

This project assumes that fields you don't have stored in Solr are available in Cassandra and they are taken from there when creating a response to a search query.

Fields in Solr and Cassandra have to have the same names.


Limitations
-----------

- The field that connects Solr and Cassandra _has_ to be declared 'int' in Solr schema and 'varint' in Cassandra schema.
- All fields that are pulled from cassandra have to be strings
