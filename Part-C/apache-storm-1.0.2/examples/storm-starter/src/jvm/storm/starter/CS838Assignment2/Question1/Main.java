package storm.starter.CS838Assignment2.Question1;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Main {
	public static void main(String[] args) {
        String consumerKey = "k0igr9N6RQzmuKzq7v3VFd5e6"; 
        String consumerSecret = "5qToFqBegZkWvbxxiOoh223ugwrLq8QKPIbVKMsC5BqnhrX1tS"; 
        String accessToken = "605679174-EFFEePsAAesyRHRHsIEqEB2eng89ChkJIZYfaM5h"; 
        String accessTokenSecret = "z06bybvd1YPPbEJttmbM5AjfBHkT1PVpnx0K5Exkfgep2";
        
        String outputFilepath = args[0];
        
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitter", new TwitterFilteredKeywordSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords));
        builder.setBolt("print", new FileWriterBolt(outputFilepath))
                .shuffleGrouping("twitter");
                
                
        Config conf = new Config();
        
        // TODO: Handle cluster mode as well.
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(120000);
        cluster.shutdown();
    }
}
