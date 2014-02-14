wget http://apache.mirror.quintex.com/lucene/solr/4.6.1/solr-4.6.1.tgz
tar -xvzf solr-4.6.1.tgz
cp -r solr-overwrite/* solr-4.6.1

ant
sed "s@/home/minmax/zgit/solr-cassandra-bridge@$PWD@" solr-overwrite/example/solr/solr.xml >solr-4.6.1/example/solr/solr.xml


cd solr-4.6.1/example/
java -jar start.jar &
sleep 10
cd myexamples
java -jar post.jar docs-to-index.xml

echo "START USING SOLR"
echo "Open http://localhost:8983/solr/#/collection1/query?q=title:article"
cd ../../../
