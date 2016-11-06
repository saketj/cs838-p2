package storm.starter.CS838Assignment2.Question1;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
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
        
        if (outputFilepath.startsWith("hdfs")) {
            RecordFormat format = new DelimitedRecordFormat();
            SyncPolicy syncPolicy = new CountSyncPolicy(1000);
            FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
            FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/user/ubuntu/cs-838/part-c/question-1/output/");
            
            HdfsBolt hdfsBolt = new HdfsBolt();
            hdfsBolt.withFsUrl("hdfs://10.254.0.147:8020")
            	    .withFileNameFormat(fileNameFormat)
            	    .withRecordFormat(format)
            	    .withSyncPolicy(syncPolicy)
            	    .withRotationPolicy(rotationPolicy);
            builder.setBolt("hdfs", hdfsBolt).shuffleGrouping("twitter");
        } else {
            builder.setBolt("print", new LocalFileWriterBolt(outputFilepath))
                   .shuffleGrouping("twitter");
        }
        
                
                
        Config conf = new Config();
        
        // TODO: Handle cluster mode as well.
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(120000);
        cluster.shutdown();
    }
}
