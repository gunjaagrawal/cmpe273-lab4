package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * Distributed cache service
 * 
 */
public class DistributedCacheService implements CacheServiceInterface {
    private final String [] cacheServerUrls;

    public DistributedCacheService(String [] serverUrls) {
        this.cacheServerUrls = serverUrls;
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key) {
    	// Map to hold this key's values mapping with servers
    	Map<String, List<String>> valueServerMap = new HashMap<String, List<String>>();
    	
    	// Map to hold future handlers for async get calls
    	Map<String, Future<HttpResponse<JsonNode>>> futures = new HashMap<String, Future<HttpResponse<JsonNode>>>();
    	
    	// Map async get request to all servers
    	for (String cacheServerUrl : cacheServerUrls) {
    		Future<HttpResponse<JsonNode>> future = Unirest.get(cacheServerUrl + "/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
      			    .asJsonAsync();
    		futures.put(cacheServerUrl, future);
    	}
    	
    	// Track what is the maximum freq'd "value" returned across servers, 
    	// and if its majority? (for figuring out quorum, and do repair-on-read)
    	String maxValue = null;
    	int maxValueCount = 0;
    	for (Entry<String, Future<HttpResponse<JsonNode>>> future : futures.entrySet()) {
    		try {
    			// Get "value" for the "key" from current server, and update corresponding 
    			// mapping
    			HttpResponse<JsonNode> response = future.getValue().get();
    			if (response.getCode() == 200) {
	    			String value = response.getBody().getObject().getString("value");
	    			List<String> serversWithThisValue = null;
	    			if (valueServerMap.containsKey(value)) {
	    				serversWithThisValue = valueServerMap.get(value);
	    			} else {
	    				serversWithThisValue = new ArrayList<String>();
	    				valueServerMap.put(value, serversWithThisValue);
	    			}
	    			serversWithThisValue.add(future.getKey());
	    			int count = serversWithThisValue.size();
	    			if (count > maxValueCount) {
	    				maxValueCount = count;
	    				maxValue = value;
	    			}
	    				
    			}
    		} catch (Exception e) {
    			System.err.println(e);
    		}
    	}
    	
    	// If the count of servers for the maximumly available value is not majority 
    	// .. something serious is wrong, because we don't have quorum. throw exception
    	// .. because we cannot proceed with repair-on-read
    	if (maxValueCount < cacheServerUrls.length / 2) {
    		throw new RuntimeException("Quorum not reached for value of this key. "
    				+ " Cluster might be unstable");
    	}
    	
    	// Update the servers that don't have the quorum value
    	// .. ie. do repair-on-read
    	for (Entry<String, List<String>> valueServersPair : valueServerMap.entrySet()) {
    		String value = valueServersPair.getKey();
    		if (!maxValue.equals(value)) {
    			for (String cacheServerUrl : valueServersPair.getValue()) {
	    			HttpResponse<JsonNode> response = null;
	    	        try {
	    	            response = Unirest
	    	                    .put(cacheServerUrl + "/cache/{key}/{value}")
	    	                    .header("accept", "application/json")
	    	                    .routeParam("key", Long.toString(key))
	    	                    .routeParam("value", maxValue).asJson();
	    	        } catch (UnirestException e) {
	    	            System.err.println(e);
	    	        }
	
	    	        if (response.getCode() != 200) {
	    	            System.out.println("Failed to update the cache.");
	    	        }
    			}
    		}
    	}
    	
    	// Return quorum value
    	return maxValue;
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
     *      java.lang.String)
     */
    @Override
    public void put(long key, String value) {
    	
    	// Map to hold future handlers for async put calls
    	Map<String, Future<HttpResponse<JsonNode>>> futures = new HashMap<String, Future<HttpResponse<JsonNode>>>();
    	
    	// Map async put request to all servers
    	for (String cacheServerUrl : cacheServerUrls) {
    		Future<HttpResponse<JsonNode>> future = Unirest.put(cacheServerUrl + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value)
      			    .asJsonAsync();
    		futures.put(cacheServerUrl, future);
    	}
    	
    	// Track the number of servers that successfully added key
    	int serversSuccessfullyUpdate = 0;
    	
    	// Check status of all servers, and check how many successfully added key
    	for (Entry<String, Future<HttpResponse<JsonNode>>> future : futures.entrySet()) {
    		try {
    			HttpResponse<JsonNode> response = future.getValue().get();
    			if (response.getCode() == 200) {
    				serversSuccessfullyUpdate ++;
    			} else {
    				System.out.println("Failed to add to the cache.");
    			}
    		} catch (Exception e) {
    			System.err.println(e);
    		}
    	}
    	
    	// Less than half servers updated, roll-back (delete the key)
    	if (serversSuccessfullyUpdate < cacheServerUrls.length / 2) {
    		delete(key);
    	}
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#delete(long)
     */
	@Override
	public void delete(long key) {
		// Delete the key on all servers in cluster
		for (String cacheServerUrl : this.cacheServerUrls) {
			HttpResponse<JsonNode> response = null;
	        try {
	        	response = Unirest.delete(cacheServerUrl + "/cache/{key}")
	                    .header("accept", "application/json")
	                    .routeParam("key", Long.toString(key)).asJson();
	        } catch (UnirestException e) {
	            System.err.println(e);
	        }
	
	        if (response.getCode() != 200) {
	            System.out.println("Failed to delete from the cache.");
	        }
		}
	}
}
