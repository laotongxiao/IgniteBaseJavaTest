import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

public class IgnitePartitionLossExampleServer {

    public static void main(String[] args) {
        Ignite ignite;

        if(args.length == 1 && !args[0].isEmpty())
        {
            //如果启动时指定了配置文件,则用指定的配置文件
            System.out.println("Use " + args[0] + " to start.");
            ignite = Ignition.start(args[0]);
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
        }

    }
}
