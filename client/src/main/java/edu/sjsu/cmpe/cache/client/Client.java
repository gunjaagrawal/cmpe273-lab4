package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        
        CacheServiceInterface cache = new DistributedCacheService(
                new String[] {"http://localhost:3000", 
                			  "http://localhost:3001", 
                			  "http://localhost:3002" });

        // Put <Key, Value> = <1, "a">
        // (all servers are running)
        cache.put(1, "a");
        System.out.println("put (1 => a)" );

        Thread.sleep(30000);

        // Put <Key, Value> = <1, "b">
        // (server 1 stopped during last 30 seconds sleep)
        // (only 2 servers will be updated with the next call)
        cache.put(1, "b");
        System.out.println("put (1 => b)" );

        Thread.sleep(30000);

        // Get Key = 1
        // (1 server will return a, and 2 will return b, but read-repair will happen)
        // (and all three servers will get b as value internally)
        String value = cache.get(1);
        System.out.println("get (1) => " + value);


        System.out.println("Exiting Cache Client...");
    }

}
