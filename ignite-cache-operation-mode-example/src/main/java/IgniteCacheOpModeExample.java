import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

public class IgniteCacheOpModeExample {

    public static void main(String[] args) {
        Ignite ignite;
        // 创建一个TEST缓存并写入一些数据, key是城市的名字,value是省的名字
        IgniteCache<String, String> cityProvinceCache;

        if(args.length == 1 && !args[0].isEmpty())
        {
            //如果启动时指定了配置文件,则用指定的配置文件
            System.out.println("Use " + args[0] + " to start.");
            ignite = Ignition.start(args[0]);
            //配置文件中,我们将缓存设置为partitioned模式,backup数量为1
            cityProvinceCache = ignite.getOrCreateCache("TEST");
        }
        else
        {
            //如果启动时没指定配置文件,则生成一个配置文件
            System.out.println("Create an IgniteConfiguration to start.");
            TcpDiscoverySpi spi = new TcpDiscoverySpi();
            TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
            ipFinder.setMulticastGroup("224.0.0.251");
            spi.setIpFinder(ipFinder);
            IgniteConfiguration cfg = new IgniteConfiguration();
            cfg.setDiscoverySpi(spi);
            ignite = Ignition.start(cfg);
            CacheConfiguration<String, String> cacheCfg = new CacheConfiguration("TEST");
            // 如果不用配置文件启动,缓存模式被设置为replicated
            cacheCfg.setCacheMode(CacheMode.REPLICATED);
            /* 下面的配置将"TEST"缓存设为partitioned模式,并且设置了backup数量为1,这样保证即使有一个node出现
                故障的情况下,缓存数据还是完整可用的
            cacheCfg.setCacheMode(CacheMode.PARTITIONED);
            cacheCfg.setBackups(1);
             */
            cityProvinceCache = ignite.getOrCreateCache(cacheCfg);
        }


        cityProvinceCache.put("Edmonton", "Alberta");
        cityProvinceCache.put("Markham", "Ontario");
        cityProvinceCache.put("Montreal", "Quebec");

    }
}
