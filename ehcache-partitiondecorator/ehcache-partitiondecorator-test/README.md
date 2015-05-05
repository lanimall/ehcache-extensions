JPerfTester: A Simple Java Benchmark Framework
=============================================

Overview
---------------------------------------------
This testing framework is built as a modular facade to facilitate the execution of multi-threaded operations against various JAVA libraries and tools.
It contains and abstracts boiler plates functions such as threading, timing measurements, operation life cycle, etc... and ensures that this boiler plate is contention free.
That way, client loader/benchmarking programs can simply focus on implementing the specific operations needed to "load" the tools/library under test.
Built in java, this library run wherever JAVA runs (in other words, every OS)

The main advantages of this Java testing framework are:
 - Direct execution of the multi-threaded tests against your application libraries
 - Create "loader" operations that use your own business objects, all using JAVA (no new processes / languages to learn)
 - Accurate and least intrusive timing measurements
 - Extensibility of code + framework under test (easy to add new frameworks to test against)
 - Simplicity of build / install / run
 
Sample Applications
---------------------------------------------
Several sample testing applications for [Terracotta BigMemory or Ehcache](http://terracotta.org/) have been created for quick execution. So far:
 - POJOCacheTester
 	- A sample application which demonstrate how to load, get, and search based on custom POJOs cache values (Customer --> Address --> AddressCategory)
 - CustomCacheSearchTester
	- A sample application which demonstrate how to load a cache using a custom POJO for the cache keys, and perform multi-threaded searches against the indexed attributes of these cache key objects.
 - CacheWriterTester
	- A sample application which demonstrate how to load, get, and search objects using terracotta write-behind mechanism.

Quick start
---------------------------------------------
 - Navigate to JPerfTester root folder
 - Run: mvn install
 - Navigate to the sample applications folder: <JPerfTester-ROOT>/TerracottaEhCacheTesterSuite/TesterClients
 - Each of the sample applications have a simple "extractable and runnable" package to deploy anywhere:
	- Copy the tar.gz package in "<sample application>/dist/" folder to the location of your choice and extract it.
	- [optional] modify parameters in app.properties (i.e. number of threads to use, cache name tu use, business object factory to use to load objects in cache, etc...)
	- [optional] modify ehcache.xml (i.e. max elements in memory, time to live, etc...)
	- In the extracted location, execute script "run.sh" (make sure your $JAVA_HOME is set for this script to run)

Extend and Customize
---------------------------------------------
 
If you want to create a test project for [Terracotta BigMemory or Ehcache](http://terracotta.org/), add the following in your maven pom.xml (or look at the pom.xml of each sample applications):

	...
	<dependencies>
	...
		<dependency>
			<groupId>org.terracotta.utils.jperftester.ehcache</groupId>
			<artifactId>jperftester-terracotta-ehcache-engine</artifactId>
			<version>${jperftester.version}</version>
		</dependency>
	...
	</dependencies>
	...

To refer to this library from another "non-terracotta" project (i.e. only using the multi-threading engine capability), simply add the following in your maven pom.xml (make sure to use the right version number):

	...
	<dependencies>
	...
		<dependency>
			<groupId>org.terracotta.utils.jperftester</groupId>
			<artifactId>jperftester-engine</artifactId>
			<version>${jperftester.version}</version>
		</dependency>
	...
	</dependencies>
	...