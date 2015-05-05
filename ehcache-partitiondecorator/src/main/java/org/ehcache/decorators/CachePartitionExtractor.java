package org.ehcache.decorators;

import java.util.Properties;

import net.sf.ehcache.Element;
import net.sf.ehcache.config.InvalidConfigurationException;
import net.sf.ehcache.search.attribute.AttributeExtractor;
import net.sf.ehcache.search.attribute.AttributeExtractorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachePartitionExtractor implements AttributeExtractor {
	private static final Logger log = LoggerFactory.getLogger(CachePartitionExtractor.class);

	private static final long serialVersionUID = 1L;

	private static final String PARTITIONTYPENAME = "partitionType";
	private static final String PARTITIONCOUNTNAME = "partitionCount";
	private static final String KEY = "key";
	private static final String VALUE = "value";

	private final PartitionType type;
	private final int partitionTotal;

	/**
	 * The various types of the start of the expression
	 */
	private enum PartitionType {
		VALUE, KEY;
	}

	/**
	 * Create a new CachePartitionExtactor
	 *
	 * @param expression
	 */
	public CachePartitionExtractor(Properties props) throws InvalidConfigurationException {
		if ( !props.containsKey(PARTITIONTYPENAME) )
			throw new IllegalArgumentException("property '" + PARTITIONTYPENAME + "' is required");

		String partitionType = props.getProperty(PARTITIONTYPENAME);

		if ( !props.containsKey(PARTITIONCOUNTNAME) )
			log.warn("Property '" + PARTITIONCOUNTNAME + "' is not specified - will default to 1");
		String numPartitions = props.getProperty(PARTITIONCOUNTNAME, "1");

		if(log.isDebugEnabled()){
			log.debug(String.format("Properties: %s=%s / %s=%s", PARTITIONTYPENAME, partitionType, PARTITIONCOUNTNAME, numPartitions));
		}

		if(null != partitionType){
			partitionType = partitionType.trim();
			if (partitionType.equalsIgnoreCase(KEY)) {
				type = PartitionType.KEY;
			} else if (partitionType.equalsIgnoreCase(VALUE)) {
				type = PartitionType.VALUE;
			} else {
				log.warn("Value for property '" + PARTITIONTYPENAME + "' must start with either, \"" + KEY + "\" or \"" + VALUE + "\" - not valid: " + partitionType);
				type = PartitionType.KEY;
			}
		} else {
			type = PartitionType.KEY;
		}

		partitionTotal = Integer.parseInt(props.getProperty(PARTITIONCOUNTNAME, "1"));

		if(log.isDebugEnabled()){
			log.debug(String.format("Parsed Values: %s=%s / %s=%d", PARTITIONTYPENAME, type.toString(), PARTITIONCOUNTNAME, partitionTotal));
		}
	}

	@Override
	public Object attributeFor(Element el, String attrName)
			throws AttributeExtractorException {

		Object objToPartitionOn = null;
		Integer partitionId = null;
		if(log.isDebugEnabled()){
			log.debug(String.format("%s=%s / %s=%d", PARTITIONTYPENAME, type.toString(), PARTITIONCOUNTNAME, partitionTotal));
		}

		if(type == PartitionType.KEY){
			objToPartitionOn = el.getObjectKey();
		} else if (type == PartitionType.VALUE){
			objToPartitionOn = el.getObjectValue();
		}

		if(null != objToPartitionOn){
			if (objToPartitionOn instanceof CachePartitionable) {
				CachePartitionable objToPartition = (CachePartitionable) objToPartitionOn;
				try{
					partitionId = objToPartition.getPartition(partitionTotal);
				} catch (Exception exc){
					log.error("An error occurred during the partition calculation...fail silently but will not partition...");
				}
			} else {
				if(log.isDebugEnabled()){
					log.debug("The object to partition on is not of type CachePartitionable. Make sure it implements that interface");
				}
			}
		}

		if(log.isDebugEnabled())
			log.debug(String.format("%s=%s", "Partition Id", (null != partitionId)?""+partitionId:"null"));

		return partitionId;
	}
}
