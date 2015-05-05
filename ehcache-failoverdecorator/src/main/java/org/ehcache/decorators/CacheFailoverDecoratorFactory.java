package org.ehcache.decorators;

import java.util.Properties;

import net.sf.ehcache.Ehcache;
import net.sf.ehcache.constructs.CacheDecoratorFactory;

public class CacheFailoverDecoratorFactory extends CacheDecoratorFactory{

    public Ehcache createDecoratedEhcache(Ehcache cache, Properties properties) {
    	return new CacheFailoverDecorator(cache, properties);
    }
    
    public Ehcache createDefaultDecoratedEhcache(Ehcache cache, Properties properties) {
    	throw new UnsupportedOperationException();
    }
}


