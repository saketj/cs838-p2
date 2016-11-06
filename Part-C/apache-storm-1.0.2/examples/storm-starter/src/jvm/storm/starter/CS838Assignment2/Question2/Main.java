package storm.starter.CS838Assignment2.Question2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class Main {
    
    private static final String STOPWORD_FILEPATH = "src/jvm/storm/starter/CS838Assignment2/Question2/stopwords.txt";
    
    public static void main(String[] args) throws IOException {
        String consumerKey = "k0igr9N6RQzmuKzq7v3VFd5e6"; 
        String consumerSecret = "5qToFqBegZkWvbxxiOoh223ugwrLq8QKPIbVKMsC5BqnhrX1tS"; 
        String accessToken = "605679174-EFFEePsAAesyRHRHsIEqEB2eng89ChkJIZYfaM5h"; 
        String accessTokenSecret = "z06bybvd1YPPbEJttmbM5AjfBHkT1PVpnx0K5Exkfgep2";        
        List<String> hashtags = new ArrayList<String>();
        List<Integer> friendsCount = new ArrayList<Integer>();
        for (int i = 0; i < args.length; ++i) {
            try {
        	friendsCount.add(Integer.parseInt(args[i]));
            } catch (NumberFormatException e) {            
        	System.out.println("Hashtag: " + args[i]);
        	hashtags.add(args[i]);
            }
        }
                
        // Load stopwords into a hashset
        HashSet<String> stopwords = new HashSet<String>();
        String line = null;
        BufferedReader br = new BufferedReader(new FileReader(STOPWORD_FILEPATH));
        while ((line = br.readLine()) != null) {
            stopwords.add(line.trim().toLowerCase());
        }
        br.close();
        

        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitterSpout", new TwitterSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, hashtags));
        builder.setSpout("hashtagSpout", new HashtagSpout(hashtags));
        builder.setSpout("friendsCountSpout", new FriendsCountSpout(friendsCount));
        builder.setBolt("tweetProcessorBolt", new TweetProcessorBolt(hashtags, stopwords))
                .shuffleGrouping("twitterSpout")
                .allGrouping("hashtagSpout")
                .allGrouping("friendsCountSpout");        
                
                
        Config conf = new Config();
        
        
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(120000);
        cluster.shutdown();
    }

}
