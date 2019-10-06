import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import javax.cache.CacheException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class IgnitePartitionLossExampleClient {
    private static AtomicBoolean partitionLost = new AtomicBoolean(false);

    public static void main(String[] args) {
        Ignite ignite;

        if (args.length == 1 && !args[0].isEmpty()) {
            //如果启动时指定了配置文件,则用指定的配置文件
            System.out.println("Use " + args[0] + " to start.");
            ignite = Ignition.start(args[0]);
        } else {
            //如果启动时没指定配置文件,则生成一个配置文件
            System.out.println("Create an IgniteConfiguration to start.");
            TcpDiscoverySpi spi = new TcpDiscoverySpi();
            TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
            ipFinder.setMulticastGroup("224.0.0.251");
            spi.setIpFinder(ipFinder);
            IgniteConfiguration cfg = new IgniteConfiguration();
            cfg.setDiscoverySpi(spi);
            cfg.setClientMode(true);
            //默认由于性能原因，Ignite会忽略所有事件，这里要主动配置需要监听的事件
            cfg.setIncludeEventTypes(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);
            ignite = Ignition.start(cfg);
        }

        // 创建一个TEST缓存, cache mode设为PARTITIONED, backup数量为1, 并把partition loss policy设为READ_WRITE_SAFE
        CacheConfiguration<String, String> cacheCfg = new CacheConfiguration<>();
        cacheCfg.setName("TEST");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setBackups(0);
        cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE);
        IgniteCache<String, String> cityProvinceCache = ignite.getOrCreateCache(cacheCfg);

        // Local listener that listens to local events.
        IgnitePredicate<CacheRebalancingEvent> locLsnr = evt -> {
            try {
                System.out.println("=========Received event [evt=" + evt.name() + "]==========");
                Collection<String> caches = ignite.cacheNames();
                Collection<Integer> lostPartitions = cityProvinceCache.lostPartitions();
                if (lostPartitions != null) {
                    partitionLost.set(true);
                }
                return true; // Continue listening.
            } catch (Exception e) {
                System.out.println(e);
            }
            System.out.println("=========Stop listening==========");
            return false;
        };

        // Subscribe to specified cache events occuring on local node.
        ignite.events().localListen(locLsnr,
                EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST);


        List<String> cities = new ArrayList<String>(Arrays.asList("Edmonton",
                "Calgary", "Markham", "Toronto", "Richmond Hill", "Montreal"));

        // 写入一些数据, key是城市的名字,value是省的名字
        populateCityProvinceData(cityProvinceCache);

        //用下面的while循环不停模拟对cache的读写操作
        while(true) {
            try {
                for (String city : cities) {
                    try {
                            if (!partitionLost.get()) {
                                //如果cache一切正常，则正常读
                                getAndPrintCityProvince(city, cityProvinceCache);
                            } else {
                                //如果cache出现partition lost，模拟错误处理,　我们这里简单把cache
                                //lost partiton重置，并重新写入数据
                                Collection<Integer> lostPartitions = cityProvinceCache.lostPartitions();
                                System.out.println("Cache lost partitions: " + lostPartitions.toString());
                                ignite.resetLostPartitions(Arrays.asList("TEST"));
                                populateCityProvinceData(cityProvinceCache);
                                partitionLost.set(false);
                            }
                    } catch(CacheException e) {
                        e.printStackTrace();
                    }
                }
                Thread.sleep(1000);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static void populateCityProvinceData(IgniteCache<String, String> cityProvinceCache) {
        System.out.println("Populate city province data！");
        cityProvinceCache.put("Edmonton", "Alberta");
        cityProvinceCache.put("Calgary", "Alberta");
        cityProvinceCache.put("Markham", "Ontario");
        cityProvinceCache.put("Toronto", "Ontario");
        cityProvinceCache.put("Richmond Hill", "Ontario");
        cityProvinceCache.put("Montreal", "Quebec");
    }

    private static void getAndPrintCityProvince(String city, IgniteCache<String, String> cityProvinceCache) {
        System.out.println(city + " is in " + cityProvinceCache.get(city));
    }
}
