
CREATE KEYSPACE articles WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
CREATE TABLE articles.articles (
  key varint,
  column1 ascii,
  value text,
  PRIMARY KEY (key, column1)
) WITH COMPACT STORAGE AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='NONE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

INSERT INTO articles.articles (key, column1, value) values (1001, 'title', 'Article1 Title1');
INSERT INTO articles.articles (key, column1, value) values (1001, 'body', 'Article1 Body1');
INSERT INTO articles.articles (key, column1, value) values (1002, 'body', 'Article2 Title2');
INSERT INTO articles.articles (key, column1, value) values (1002, 'body', 'Article2 Body2');
