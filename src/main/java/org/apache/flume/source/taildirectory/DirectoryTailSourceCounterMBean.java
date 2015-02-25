package org.apache.flume.source.taildirectory;

public interface DirectoryTailSourceCounterMBean {
	
	public void increaseCounterMessageSent();
	public void increaseCounterMessageSentError();
	
    public long getCounterMessageSent();
	public long getCounterMessageSentError();
    public long getCurrentThroughput();
    public long getAverageThroughput();
}
