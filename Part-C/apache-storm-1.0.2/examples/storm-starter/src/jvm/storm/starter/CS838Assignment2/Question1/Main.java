package storm.starter.CS838Assignment2.Question1;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	private static final String HDFS_ABSOLUTE_URI_PATTERN = "hdfs://[\\d\\.:]+";

	public static void main(String[] args) {
        String consumerKey = "k0igr9N6RQzmuKzq7v3VFd5e6"; 
        String consumerSecret = "5qToFqBegZkWvbxxiOoh223ugwrLq8QKPIbVKMsC5BqnhrX1tS"; 
        String accessToken = "605679174-EFFEePsAAesyRHRHsIEqEB2eng89ChkJIZYfaM5h"; 
        String accessTokenSecret = "z06bybvd1YPPbEJttmbM5AjfBHkT1PVpnx0K5Exkfgep2";
        
        String outputFilepath = args[0];
        
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 1, arguments.length);
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("twitterSpout", new TwitterFilteredKeywordSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords));
        
        if (outputFilepath.startsWith("hdfs")) {                       
            Pattern pat = Pattern.compile(HDFS_ABSOLUTE_URI_PATTERN);
            Matcher m = pat.matcher(outputFilepath);
            String fsUrl = null;
            String fsPath = null;
            if (m.find()) {
        	fsUrl = m.group(0);
        	String uri[] = outputFilepath.split(HDFS_ABSOLUTE_URI_PATTERN);
        	if (uri.length == 2) {
        	    fsPath = uri[1];
        	} else {
        	    System.err.println("Expected absolute HDFS uri, e.g. hdfs://10.254.0.147:8020/user/ubuntu/output/");
        	    System.exit(1);
        	}
            } else {
        	System.err.println("Expected absolute HDFS uri, e.g. hdfs://10.254.0.147:8020/user/ubuntu/output/");
        	System.exit(1);
            }
            
            RecordFormat format = new DelimitedRecordFormat();
            SyncPolicy syncPolicy = new CountSyncPolicy(1000);
            FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.GB);
            FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(fsPath);
                                    
            HdfsBolt hdfsBolt = new HdfsBolt();
            hdfsBolt.withFsUrl(fsUrl)
            	    .withFileNameFormat(fileNameFormat)
            	    .withRecordFormat(format)
            	    .withSyncPolicy(syncPolicy)
            	    .withRotationPolicy(rotationPolicy);
            builder.setBolt("hdfsBolt", hdfsBolt).shuffleGrouping("twitterSpout");
        } else {
            builder.setBolt("localFileWriterBolt", new LocalFileWriterBolt(outputFilepath))
                   .shuffleGrouping("twitterSpout");
        }
        
                
                
        Config conf = new Config();
        
        // TODO: Handle cluster mode as well.
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
        
        Utils.sleep(120000);
        cluster.shutdown();
    }
}
