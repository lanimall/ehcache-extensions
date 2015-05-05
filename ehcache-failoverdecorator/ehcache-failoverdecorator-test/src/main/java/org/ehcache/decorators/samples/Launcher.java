package org.ehcache.decorators.samples;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import net.sf.ehcache.Cache;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {
	private static Logger log = LoggerFactory.getLogger(Launcher.class);
	private static final String CACHE_NAME_DEFAULT = "Users";
	public static final String ENV_CACHE_NAME = "ehcache.config.cachename";

	private Random rdm = new Random(System.currentTimeMillis());

	public static final long DEFAULT_MULTI_OPERATION_SLEEPINTERVAL = 5000;
	public static final String LOOP_MODIFIER = "{loop}";
	public static final String ARGS_SEPARATOR = " ";
	public static final String COMMAND_SEPARATOR = ",";

	private static int operationIdCounter = 1;
	private enum OPERATIONS {
		OP_LOAD("Load cache with elements (@@opInput@@ <number of elements>)"),
		OP_GETSINGLE("Display a cache element (@@opInput@@ <key>)"),
		OP_GETALL("Display all cache elements (@@opInput@@ <ExpiryCheck=true|false>)"),
		OP_CONTAINSKEY("Check if key is in cache (@@opInput@@ <key> <ExpiryCheck=true|false>)"),
		OP_ADDNEW("Add new element to cache (@@opInput@@ <user name>)"),
		OP_UPDATE("Update an existing cache element (@@opInput@@ <key to update> <new user name>)"),
		OP_DELETESINGLE("Delete a cache element by key (@@opInput@@ <key to delete>)"),
		OP_DELETEALL("Remove all cache entries"),
		OP_QUIT('Q', "Quit program");

		private char opInput;
		private String opDetail;

		private OPERATIONS() {
			this(operationIdCounter++, "");
		}

		private OPERATIONS(String opDetail) {
			this(operationIdCounter++, opDetail);
		}

		private OPERATIONS(int opInput, String opDetail) {
			this(new Integer(opInput).toString().charAt(0), opDetail);
		}

		private OPERATIONS(char opInput, String opDetail) {
			this.opInput = opInput;
			if(null != opDetail){
				opDetail = opDetail.replaceAll("@@opInput@@", String.valueOf(opInput));
			}
			this.opDetail = opDetail;
		}

		@Override
		public String toString() {
			return String.valueOf(opInput) + " - " + opDetail;
		}

		public static OPERATIONS getById(char input){
			for(OPERATIONS op : values()){
				if(op.opInput == input)
					return op;
			}
			return null;
		}
	}

	Ehcache cache = null;
	int DEFAULT_NBOFELEMENTS = 10;
	String default_value = "Terracotta";

	public Launcher() throws Exception {
		String cacheName = "";
		if(null != System.getProperty(ENV_CACHE_NAME)){
			cacheName = System.getProperty(ENV_CACHE_NAME);
		} else {
			cacheName = CACHE_NAME_DEFAULT;
		}

		cache = CacheUtils.getCache(cacheName);
		if (cache == null) {
			System.out.println("Could not find the cache " + cacheName + ". Exiting.");
			System.exit(0);
		}
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new Launcher();
		launcher.run(args);
		System.out.println("Completed");
		System.exit(0);
	}

	public void run(String[] args) throws Exception {
		boolean keepRunning = true;
		while (keepRunning) {
			if(null != args && args.length > 0){
				//there could be several command chained together with comma...hence let's try to find it out
				String joinedCommand = joinStringArray(args, ARGS_SEPARATOR);

				boolean doLoop = false;
				if(joinedCommand.startsWith(LOOP_MODIFIER)){
					doLoop = true;
					joinedCommand = joinedCommand.substring(LOOP_MODIFIER.length());
				}

				if(log.isDebugEnabled())
					log.debug("Full command: " + joinedCommand);

				String[] multipleCommands = joinedCommand.split(COMMAND_SEPARATOR);
				do{
					for(String inputCommand : multipleCommands){
						processInput(inputCommand);
						Thread.sleep(DEFAULT_MULTI_OPERATION_SLEEPINTERVAL);
					}
				} while(doLoop);

				//if args are specified directly, it should run once and exit (useful for batch scripting)
				keepRunning = false;
			} else {
				printOptions();
				String input = getInput();
				if (input.length() == 0) {
					continue;
				}

				if(log.isDebugEnabled())
					log.debug("Full command: " + input);

				String[] multipleCommands = input.split(COMMAND_SEPARATOR);
				for(String inputCommand : multipleCommands){
					keepRunning = processInput(inputCommand);
					if(!keepRunning)
						break;
				}
			}
		}
	}

	private String joinStringArray(String[] arr, String separator){
		String join = "";
		if(null != arr){
			for(String s : arr){
				if(join.length() > 0)
					join += separator;
				join += s;
			}
		}
		return join;
	}

	private String getInput() throws Exception {
		System.out.println(">>");

		// option1
		Scanner sc = new Scanner(System.in);
		sc.useDelimiter(System.getProperty("line.separator"));
		return sc.nextLine();
	}

	public boolean processInput(String input) throws Exception{
		String[] inputs = input.split(" ");
		return processInput(inputs);
	}

	public boolean processInput(String[] args) throws Exception{
		String[] inputArgs = null;
		String inputCommand = "";
		if(null != args && args.length > 0){
			inputCommand = args[0];
			if(args.length > 1){
				inputArgs = Arrays.copyOfRange(args, 1, args.length);
			}
		}

		return processInput(inputCommand, inputArgs);
	}

	public void printLineSeparator(){
		String lineSeparator = System.getProperty("line.separator");
		byte[] bytes = lineSeparator.getBytes();
		StringBuilder sb = new StringBuilder();
		for (byte b : bytes) {
			sb.append(String.format("%02X", b) + " ");
		}
		System.out.println("Line separator = " + lineSeparator + " (hex = " + sb.toString() + ")");
	}

	public void printOptions() {
		System.out.println();
		System.out.println("What do you want to do now?");
		for(OPERATIONS op : OPERATIONS.values()){
			System.out.println(op.toString());
		}
	}

	public boolean processInput(String input, String[] args) throws Exception {
		if(null == input || "".equals(input)){
			System.out.println("Unrecognized entry...");
			return true; 
		}

		String cacheKey = null;
		String cacheValue = null;
		boolean doExpiryCheck = true;

		//get the operation based on input
		OPERATIONS opInput = OPERATIONS.getById(input.charAt(0));
		System.out.println(String.format("Processing command: \"%s\" with params \"%s\"", opInput.opDetail, joinStringArray(args, ARGS_SEPARATOR)));

		switch (opInput) {
		case OP_LOAD:
			int nbOfElements = DEFAULT_NBOFELEMENTS;
			if(null != args && args.length > 0){
				try{
					nbOfElements = Integer.parseInt(args[0]);
				} catch (NumberFormatException nfe){
					nbOfElements = DEFAULT_NBOFELEMENTS;
				}
			}

			loadCache(nbOfElements);
			break;
		case OP_CONTAINSKEY:
			if(null != args && args.length > 0){
				cacheKey = args[0];
				if(args.length > 1)
					doExpiryCheck = Boolean.parseBoolean(args[1]);
			}

			containsCacheElement(cacheKey, doExpiryCheck);
			break;
		case OP_GETSINGLE:
			if(null != args && args.length > 0){
				cacheKey = args[0];
			}

			getCacheElement(cacheKey);
			break;
		case OP_GETALL:
			if(null != args && args.length > 0){
				doExpiryCheck = Boolean.parseBoolean(args[0]);
			}

			displayAllCacheElements(doExpiryCheck);
			break;
		case OP_ADDNEW:
			if(null != args && args.length > 0){
				cacheValue = args[0];
			}

			addNewElementToCache(cacheValue);
			break;
		case OP_UPDATE:
			if(null != args && args.length > 0){
				cacheKey = args[0];
				if(args.length > 1)
					cacheValue = args[1];
			}

			updateExistingCacheElement(cacheKey, cacheValue);
			break;
		case OP_DELETESINGLE:
			if(null != args && args.length > 0){
				cacheKey = args[0];
			}

			deleteCacheElement(cacheKey);
			break;
		case OP_DELETEALL:
			deleteAllCacheElements();
			break;
		case OP_QUIT:
			return false;
		default:
			System.out.println(String.format("Unrecognized command: %s %s", opInput.opInput, joinStringArray(args, ARGS_SEPARATOR)));
			break;
		}

		return true;
	}

	private static final String[] vals = {"a","b","c","d","e","f","g","h","i","j",
		"k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
	private static final int randomTextLength = 10;
	
	private String generateRandomKey(){
		return "USER-" + rdm.nextInt();
	}
	
	private String generateRandomUserName(){
		StringBuffer returnVal = new StringBuffer();
		for(int lp = 0;lp < randomTextLength; lp++){
			returnVal.append(vals[rdm.nextInt(vals.length)]);
		}
		return returnVal.toString();
	}

	private void loadCache(int nbOfElements) throws Exception {
		for (int i = 0; i < nbOfElements; i++) {
			add(generateRandomKey(), new User(Integer.toString(cache.getSize() + 1), generateRandomUserName()));
		}
	}

	@SuppressWarnings("unchecked")
	private void displayAllCacheElements(boolean doExpiryCheck) throws Exception {
		List<Object> keys = getAllKeys(doExpiryCheck);
		if(null != keys){
			for (Object key : keys) {
				User user = get(key);
				System.out.println("key = " + key + "; value = " + ((null != user)?user:"null"));
			}
		}
		System.out.println("Total no.of element in the cache = " + cache.getSize());
	}

	private void addNewElementToCache(String value) throws Exception {
		String key = generateRandomKey();
		String userId = Integer.toString(cache.getSize() + 1);
		User user = new User (
				userId,
				(null != value && !"".equals(value))?value:"User " + userId
				);

		add(key, user);
		System.out.println("Successfully added cache entry " + user + " with key " + key);
	}

	private void updateExistingCacheElement(String keyToUpdate, String valueToUpdate) throws Exception {
		User user = get(keyToUpdate);
		if(null != user){
			if(null != valueToUpdate && !"".equals(valueToUpdate))
				user.setName(valueToUpdate);
			else
				user.setName(user.getName() + "-" + Integer.toString(cache.getSize() + 1));

			add(keyToUpdate, user);
			System.out.println("Successfully updated cache entry with key " + keyToUpdate + " to " + user);
		} else {
			System.out.println("Could not find cache entry with key " + keyToUpdate);
		}
	}

	private void deleteCacheElement(String keyToDelete) throws Exception {
		remove(keyToDelete);
		System.out.println("Successfully deleted user from cache with key=" + keyToDelete);
	}

	private void deleteAllCacheElements() throws Exception {
		remove(null);
		System.out.println("Successfully deleted all entries from cache");
	}

	private void getCacheElement(String keyToGet) throws Exception {
		User user = get(keyToGet);
		System.out.println("Successfully retrieved key from cache [key=" + keyToGet + "/ value=" + user + "]");
	}

	private void containsCacheElement(String keyToSearch, boolean doExpiryCheck) throws Exception {
		if(containsCacheKey(keyToSearch, doExpiryCheck)){
			System.out.println("Key " + keyToSearch + " is in cache" + ((doExpiryCheck)?" and non-expired":" but could be expired"));
		} else {
			System.out.println("Key " + keyToSearch + " is not in cache");
		}
	}

	private boolean containsCacheKey(Object key, boolean expiryCheck) throws Exception {
		boolean isKeyInCache = false;
		if(expiryCheck){
			isKeyInCache = cache.isKeyInCache(key);
		} else {
			Element element = cache.get(key);
			isKeyInCache = (null != element);
		}

		return isKeyInCache;
	}

	private void add(Object key, User user) throws Exception {
		cache.put(new Element(key, user));
	}

	private User get(Object key) throws Exception {
		User user = null;
		Element element = cache.get(key);
		if (null != element) {
			user = (User) element.getObjectValue();
		}
		return user;
	}

	@SuppressWarnings("rawtypes")
	private List getAllKeys(boolean doExpiryCheck){
		List keys = null;
		if(!doExpiryCheck)
			keys = cache.getKeys();
		else
			keys = cache.getKeysWithExpiryCheck();

		return keys;
	}

	private void remove(Object key) throws Exception {
		if(null == key)
			cache.removeAll();
		else
			cache.remove(key);
	}
}