package org.apache.flume.source.taildirectory;

public interface DirectoryTailSourceCounterMBean {
	public long increaseCounterMessageSent();

    public long getCounterMessageSent();
	public long getLastSent();
	public long increaseCounterMessageSentError();
	public long getCounterMessageSentError();
	public long setStartTime();
	public long getStartTime();
    public long getSendThroughput();
}
