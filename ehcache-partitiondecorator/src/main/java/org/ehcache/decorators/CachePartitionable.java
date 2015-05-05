package org.ehcache.decorators;

public interface CachePartitionable {
	int getPartition(int numPartitions);
}
