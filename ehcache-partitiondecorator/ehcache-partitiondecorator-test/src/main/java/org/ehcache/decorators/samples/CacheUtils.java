package org.ehcache.decorators.samples;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Fabien Sanglier
 * 
 */
public class CacheUtils {
	private static Logger log = LoggerFactory.getLogger(CacheUtils.class);

	public static final String ENV_CACHE_CONFIGPATH = "ehcache.config.path";

	public static Ehcache getCache(String cacheName) throws Exception {
		return getCache(cacheName, null);
	}

	public static Ehcache getCache(String cacheName, String cacheMgrResourcePath) throws Exception {
		CacheManager cacheManager = getCacheManager(cacheMgrResourcePath);
		if (null == cacheName || !cacheManager.cacheExists(cacheName)){
			log.info("EhCache xml not found or cache not found.");
			return null;
		}

		return cacheManager.getEhcache(cacheName);
	}

	public static CacheManager getCacheManager(){
		return getCacheManager(null);
	}
	
	public static CacheManager getCacheManager(String resourcePath){
		String configLocationToLoad = null;
		if(null != resourcePath && !"".equals(resourcePath)){
			configLocationToLoad = resourcePath;
		} else if(null != System.getProperty(ENV_CACHE_CONFIGPATH)){
			configLocationToLoad = System.getProperty(ENV_CACHE_CONFIGPATH);
		}
		
		if(null != configLocationToLoad){
			InputStream inputStream = null;
			try {
				if(configLocationToLoad.indexOf("file:") > -1){
					inputStream = new FileInputStream(configLocationToLoad.substring("file:".length()));
				}else if(configLocationToLoad.indexOf("classpath:") > -1){
					inputStream = CacheUtils.class.getClassLoader().getResourceAsStream(configLocationToLoad.substring("classpath:".length()));
				} else { //default to classpath if no prefix is specified
					inputStream = CacheUtils.class.getClassLoader().getResourceAsStream(configLocationToLoad);
				}

				if (inputStream == null) {
					throw new FileNotFoundException("File at '" + configLocationToLoad	+ "' not found");
				}
				
				log.info("Loading Cache manager from " + configLocationToLoad);
				return CacheManager.create(inputStream);
			} catch(IOException ioe) {
				throw new CacheException(ioe);
			}
			finally{
				if(null != inputStream){
					try {
						inputStream.close();
					} catch (IOException e) {
						throw new CacheException(e);
					}
					inputStream = null;
				}
			}
		} else{
			log.info("Loading Cache manager from default classpath");
			return CacheManager.getInstance();
		}
	}
}
