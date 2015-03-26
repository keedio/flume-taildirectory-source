/***************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 ****************************************************************/
package org.apache.flume.source.taildirectory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.HashSet;
import java.util.Set;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DirectoryTailSource extends AbstractSource implements
		Configurable, EventDrivenSource {

	private static final String CONFIG_DIRS = "dirs";
	private static final String CONFIG_PATH = "path";
	private static final String UNLOCK_TIME = "unlockFileTime";

	private static final Logger logger = LoggerFactory
			.getLogger(DirectoryTailSource.class);
	private String confDirs;
	private Set<String> dirs;
	private Set<WatchDir> watchDirs;
	private long timeToUnlockFile;
	private DirectoryTailSourceCounter counter;

	public void configure(Context context) {
		logger.info("Source Configuring..");

		confDirs = context.getString(CONFIG_DIRS).trim();
		Preconditions.checkState(confDirs != null,
				"Configuration must be specified directory(ies).");

		String[] confDirArr = confDirs.split(" ");
		Preconditions.checkState(confDirArr.length > 0, CONFIG_DIRS
				+ " must be specified at least one.");

		timeToUnlockFile = context.getLong(UNLOCK_TIME, 1L);

		dirs = new HashSet<String>();

		for (int i = 0; i < confDirArr.length; i++) {
			String path = context.getString(CONFIG_DIRS + "." + confDirArr[i]
					+ "." + CONFIG_PATH);
			dirs.add(path);
			if (path == null) {
				logger.warn("Configuration is empty : " + CONFIG_DIRS + "."
						+ confDirArr[i] + "." + CONFIG_PATH);
				continue;
			}
		}

		counter = new DirectoryTailSourceCounter("SOURCE.DirectoryTailSource-"
				+ this.getName());
	}

	@Override
	public void start() {
		logger.info("Source Starting..");
		watchDirs = new HashSet<WatchDir>();
		counter.start();

		try {
			for (String path : dirs) {
				WatchDir watchDir = new WatchDir(FileSystems.getDefault()
						.getPath(path), this, timeToUnlockFile, counter);
				//watchDir.proccesEvents();
				watchDirs.add(watchDir);
			}
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		}

		super.start();
	}

	@Override
	public void stop() {
		counter.stop();
		logger.info("DirectoryTailSource {} stopped. Metrics: {}", getName(),
				counter);
		for (WatchDir watchDir : watchDirs) {
			watchDir.stop();
		}
		super.stop();
	}
}
