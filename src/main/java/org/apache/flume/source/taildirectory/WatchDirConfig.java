package org.apache.flume.source.taildirectory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchDirConfig {
	private static final Logger LOGGER = LoggerFactory.getLogger(WatchDirConfig.class);
	private String dir;
	private String filenamePattern;
	
	public WatchDirConfig(String dir, String filenamePattern) {
		this.dir = dir;
		this.filenamePattern = filenamePattern;
	}
	
	public String getDir() {
		return dir;
	}
	
	public void setDir(String dir) {
		this.dir = dir;
	}
	
	public String getFilenamePattern() {
		return filenamePattern;
	}
	
	public void setFilenamePattern(String filenamePattern) {
		this.filenamePattern = filenamePattern;
	}
}
