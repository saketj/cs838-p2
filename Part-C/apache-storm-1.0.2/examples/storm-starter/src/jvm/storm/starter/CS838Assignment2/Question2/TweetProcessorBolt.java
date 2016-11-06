package storm.starter.CS838Assignment2.Question2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import twitter4j.HashtagEntity;
import twitter4j.Status;

@SuppressWarnings("serial")
public class TweetProcessorBolt extends BaseRichBolt {       
    long _tweetCounter;
    List<String> _hashtags;
    int _friendsCount;
    int _emitCounter;
    HashSet<String> _bufferedTweets;
    HashSet<String> _bufferedWords;
    HashSet<String> _stopwords;
    OutputCollector _collector;    
    final static int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 30;    
    
    public TweetProcessorBolt(List<String> hashtags, HashSet<String> stopwords) {
	_emitCounter = 0;
	_tweetCounter = 0;
	this._hashtags = new ArrayList<String>();
	this._friendsCount = 0;
	this._bufferedTweets = new HashSet<String>();
	this._bufferedWords = new HashSet<String>();
	this._stopwords = stopwords;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
      this._collector = collector;      
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void execute(Tuple tuple) {
	if (TupleUtils.isTick(tuple)) {	      
	      emitTweets();	      
	} else if (tuple.getSourceComponent().equals("hashtagSpout")) {	    
	    List<String> randomHashtags = (List<String>) tuple.getValue(0);
	    System.out.print("Random hashtags from spout: ");
	    for (String value : randomHashtags) {
		System.out.print(value + "\t");		
	    }
	    System.out.println();
	    _hashtags.clear();
	    _hashtags.addAll(randomHashtags);
	    Collections.sort(_hashtags);
	} else if (tuple.getSourceComponent().equals("friendsCountSpout")) {	    
	    Integer friendCount = (Integer) tuple.getValue(0);
	    System.out.println("Random friendCount from spout: " + friendCount);
	    _friendsCount = friendCount;
	} else {	    
    	    Status tweet = (Status) tuple.getValueByField("tweet");
    	    for (HashtagEntity hashtagEntity : tweet.getHashtagEntities()) {
    		// Check if any of the hashtags value match the current set of hashtags
    		if (Collections.binarySearch(_hashtags, hashtagEntity.getText().toLowerCase()) >= 0
    			&& tweet.getUser().getFriendsCount() < _friendsCount) {
    		    String tweetText = tweet.getText();
    		    processTweetWords(tweetText);
    		    _bufferedTweets.add(tweetText);
    		    ++_tweetCounter;
    		}
    	    }
	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
	ofd.declare(new Fields("emitCounter", "word"));
    }
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
      Map<String, Object> conf = new HashMap<String, Object>();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
      return conf;
    }
    
    private void processTweetWords(String tweet) {
	String words[] = tweet.split("\\s+");
	for (String word : words) {	    
	    if (!isStopWord(word.trim().toLowerCase())) {
		_bufferedWords.add(word.trim().toLowerCase());
	    }
	}
    }    
    
    private void emitTweets() {
	System.out.println("Emitting tweets for emit counter : " + formatCounter(_emitCounter));
	for (String tweet : _bufferedTweets) {
	    //System.out.println(tweet);
	}
	for (String word : _bufferedWords) {
	    System.out.print(word + ",");
	    _collector.emit(new Values(_emitCounter, word));   
	}
	System.out.println();
	++_emitCounter;
	_bufferedTweets.clear();
	_bufferedWords.clear();
    }
    
    private boolean isStopWord(String word) {
	if (word.length() == 0) return true;
	if (_stopwords.contains(word)) return true;
	for (int i = 0; i < word.length(); ++i) {
	    if (!Character.isAlphabetic(word.charAt(i))) {
		return true;
	    }
	}
	return false;
    }
           
    private String formatCounter(int counter) {
	StringBuilder sb = new StringBuilder();
	if (counter <= 9) {
	    sb.append("00" + counter);
	} else if (counter <= 99) {
	    sb.append("0" + counter);
	} else {
	    sb.append(counter);
	}
	return sb.toString();
    }
}