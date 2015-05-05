package org.ehcache.decorators.samples;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import org.ehcache.decorators.CachePartitionable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class User implements Serializable, CachePartitionable {
	private static Logger log = LoggerFactory.getLogger(User.class);
	
	private static final long serialVersionUID = 1L;
	private String id;
	private String name;

	public User(String id, String name) {
		super();
		this.id = id;
		this.name = name;
	}
	
	public User(String id) {
		this(id, "User " + id);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + "]";
	}

	private static final char[] partitionValues = {'a','b','c','d','e','f','g','h','i','j',
		'k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'};

	/*
	 * Let's partition on first letter of name...
	 * @see org.terracotta.utils.CachePartitionable#getPartition(int)
	 */
	@Override
	public int getPartition(int numPartitions) {
		if(numPartitions < 1 || numPartitions > partitionValues.length)
			throw new IllegalArgumentException("Partition number should be between 0 (excluded) and " + partitionValues.length);
		
		if(null == getName())
			throw new IllegalArgumentException("This partition algorithm can only work if name is specified");
		
		char firstLetter = getName().trim().charAt(0);
		int position = Arrays.binarySearch(partitionValues, firstLetter);
		
		if(log.isDebugEnabled())
			log.debug(String.format("First letter is '%s' - binary search result is:%d",firstLetter,position));
		
		if(position < 0){
			return 0;
		}
		
		int partitionId = (int)(position * numPartitions / partitionValues.length);
		if(log.isDebugEnabled())
			log.debug(String.format("PartitionId for name '%s'=%d", getName(), partitionId));
		
		return partitionId;
	}
	
	public static void main(String[] args) throws Exception {
		Random rdm = new Random(System.currentTimeMillis());
		
		User user = new User("USER-" + rdm.nextInt(), args[0]);
		int partition = user.getPartition(3);
		System.out.println("partition id = " + partition);
		System.exit(0);
	}
}