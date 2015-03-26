package org.apache.flume.source.taildirectory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class DirectoryTailSourceCounter extends MonitoredCounterGroup implements
		DirectoryTailSourceCounterMBean {

	private long startTime;

	private static final String COUNTER_MESSAGE_SENT = "source.counter.message.sent";
	private static final String COUNTER_MESSAGE_SENT_ERROR = "source.counter.message.sent.error";
	private static final String AVERAGE_THROUGHPUT = "source.average.throughput";
	private static final String CURRENT_THROUGHPUT = "source.current.throughput";

	private final ScheduledExecutorService scheduler = Executors
			.newScheduledThreadPool(1);

	private static final String[] ATTRIBUTES = { COUNTER_MESSAGE_SENT,
			COUNTER_MESSAGE_SENT_ERROR, CURRENT_THROUGHPUT, AVERAGE_THROUGHPUT };

	public DirectoryTailSourceCounter(String name) {
		super(MonitoredCounterGroup.Type.SOURCE, name, ATTRIBUTES);
		startTime = System.currentTimeMillis() / 1000;

		// Start running current throughput calculate every second
		final Runnable runnableThroughput = new ThroughputCalculate();
		scheduler.scheduleAtFixedRate(runnableThroughput, 0, 1,
				TimeUnit.SECONDS);
	}

	@Override
	public void increaseCounterMessageSent() {
		increment(COUNTER_MESSAGE_SENT);
	}

	@Override
	public long getCounterMessageSent() {
		return get(COUNTER_MESSAGE_SENT);
	}

	@Override
	public void increaseCounterMessageSentError() {
		increment(COUNTER_MESSAGE_SENT_ERROR);
	}

	@Override
	public long getCounterMessageSentError() {
		return get(COUNTER_MESSAGE_SENT_ERROR);
	}

	@Override
	public long getAverageThroughput() {
		return get(AVERAGE_THROUGHPUT);
	}

	@Override
	public long getCurrentThroughput() {
		return get(CURRENT_THROUGHPUT);
	}

	private class ThroughputCalculate implements Runnable {

		private long previousMessages = 0;
		private long currentMessages = 0;
		private long currentThroughput = 0;
		private long currentTime = 0;
		private long averageThroughput = 0;

		@Override
		public void run() {
			currentMessages = get(COUNTER_MESSAGE_SENT);
			if (currentMessages >= previousMessages) {
				currentThroughput = currentMessages - previousMessages;

				set(CURRENT_THROUGHPUT, currentThroughput);
				currentTime = System.currentTimeMillis() / 1000;

				if (currentTime > startTime) {
					averageThroughput = currentMessages
							/ (currentTime - startTime);
				}
				set(AVERAGE_THROUGHPUT, averageThroughput);
				previousMessages = currentMessages;
			}
		}
	}
}
