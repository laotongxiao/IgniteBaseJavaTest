package com.peppapigdaddy.ignite.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class IgniteEntryProcessorExample {
    public static void main(String[] args) {
        // start an ignite cluster
        Ignite ignite = startCluster(args);

        CacheConfiguration<String, City> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("CITY");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(1);
        IgniteCache<String, City> cityProvinceCache = ignite.getOrCreateCache(cacheCfg);

        // let's create a city and put it in the cache
        City markham = new City("Markham", "Ontario", 0);
        cityProvinceCache.put(markham.getCityName(), markham);
        System.out.println("Insert " + markham.toString());

        // submit two tasks to increase population
        ExecutorService service = Executors.newFixedThreadPool(2);
        IncreaseCityPopulationTask task1 = new IncreaseCityPopulationTask(cityProvinceCache, markham.getCityName(), 10000);
        IncreaseCityPopulationTask task2 = new IncreaseCityPopulationTask(cityProvinceCache, markham.getCityName(), 20000);
        Future<?> result1 = service.submit(task1);
        Future<?> result2 = service.submit(task2);
        System.out.println("Submit two tasks to increase the population");

        service.shutdown();
        try {
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // get the population and check whether it is 30000
        City city = cityProvinceCache.get(markham.getCityName());
        if (city.getPopulation() != 30000) {
            System.out.println("Oops, the population is " + city.getPopulation() + " instead of 30000");
        } else {
            System.out.println("Yeah, the population is " + city.getPopulation());
        }
    }

    public static class IncreaseCityPopulationTask implements Runnable {
        private IgniteCache<String, City> cityProvinceCache;
        private String cityName;
        private long population;

        public IncreaseCityPopulationTask(IgniteCache<String, City> cityProvinceCache,
                                          String cityName, long population) {
            this.cityProvinceCache = cityProvinceCache;
            this.cityName = cityName;
            this.population = population;
        }

        @Override
        public void run() {
            long p = 0;
            while(p++ < population) {
                cityProvinceCache.invoke(cityName, new EntryProcessor<String, City, Object>() {

                    @Override
                    public Object process(MutableEntry<String, City> mutableEntry, Object... objects)
                            throws EntryProcessorException {
                        City city = mutableEntry.getValue();
                        if (city != null) {
                            city.setPopulation(city.getPopulation() + 1);
                            mutableEntry.setValue(city);
                        }
                        return null;
                    }
                });
            }
        }
    }

    private static Ignite startCluster(String[] args) {
        if (args.length == 1 && !args[0].isEmpty()) {
            //如果启动时指定了配置文件,则用指定的配置文件
            System.out.println("Use " + args[0] + " to start.");
            return Ignition.start(args[0]);
        } else {
            //如果启动时没指定配置文件,则生成一个配置文件
            System.out.println("Create an IgniteConfiguration to start.");
            TcpDiscoverySpi spi = new TcpDiscoverySpi();
            TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
            ipFinder.setMulticastGroup("224.0.0.251");
            spi.setIpFinder(ipFinder);
            IgniteConfiguration cfg = new IgniteConfiguration();
            cfg.setDiscoverySpi(spi);
            return Ignition.start(cfg);
        }
    }
}
