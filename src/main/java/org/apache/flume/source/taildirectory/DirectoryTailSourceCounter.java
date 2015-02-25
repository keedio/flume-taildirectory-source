package org.apache.flume.source.taildirectory;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class DirectoryTailSourceCounter extends MonitoredCounterGroup implements
		DirectoryTailSourceCounterMBean {

    private long counter_message_sent;
    private long counter_message_sent_error;
    private long last_sent;
    private long start_time;
    private long sendThroughput;
    
    public static final String[] ATTRIBUTES = {
    	"counter_message_sent", "counter_message_sent_error", "last_sent", "start_time", "sendTroughput"
    };
    
    public DirectoryTailSourceCounter(String name) {
        super(MonitoredCounterGroup.Type.SOURCE, name, ATTRIBUTES);
        counter_message_sent = 0;
        counter_message_sent_error = 0;
        last_sent = 0;
        sendThroughput = 0;
        setStartTime();
    }
	
	@Override
	public long increaseCounterMessageSent() {
        last_sent = System.currentTimeMillis();
        counter_message_sent++;

        if (last_sent > start_time) {
            sendThroughput = counter_message_sent / ((last_sent - start_time) / 1000);
        }
        return counter_message_sent;
	}

	@Override
	public long getCounterMessageSent() {
		return counter_message_sent;
	}

	@Override
	public long getLastSent() {
		return last_sent;
	}

	@Override
	public long increaseCounterMessageSentError() {
		return counter_message_sent_error++;
	}

	@Override
	public long getCounterMessageSentError() {
		return counter_message_sent_error;
	}

	@Override
	public long setStartTime() {
		return start_time;
	}

	@Override
	public long getSendThroughput() {
		return sendThroughput;
	}

}
