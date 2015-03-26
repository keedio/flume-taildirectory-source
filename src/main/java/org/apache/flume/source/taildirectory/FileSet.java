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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSet {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileSet.class);
	private BufferedReader bufferedReader;
	private RandomAccessFile rReader;
	private Transaction transaction;
	private List<String> bufferList;
	private Map<String, String> headers;
	private long lastAppendTime;
	private Path filePath;
	private boolean fileIsOpen;
	private File file;

	public FileSet(Path filePath, String startFrom) throws IOException {

		this.bufferList = new ArrayList<String>();
		this.headers = new HashMap<String, String>();
		this.lastAppendTime = System.currentTimeMillis();
		this.filePath = filePath;

		file = new File(filePath.toString());
		
		if (startFrom.equals("end")){
			fileIsOpen = false;
		}
		else{
			rReader = new RandomAccessFile(file, "r");
			fileIsOpen = true;
			if (startFrom.equals("begin")) {
				rReader.seek(0);
			} else if (startFrom.equals("lastLine")) {
				seekToLastLine(rReader);
			}
	
			LOGGER.debug("File length --> " + file.length());
			LOGGER.debug("File pointer --> " + rReader.getFilePointer());
			LOGGER.debug("FileSet has been created " + filePath);
		}
	}

	// This method is use to avoid lost last line log
	private void seekToLastLine(RandomAccessFile rReader) throws IOException {

		long fileLength = rReader.length() - 1;
		long filePointer = fileLength;
		boolean posReached = false;
		int readByte = 0;

		while (filePointer != -1 && !posReached) {
			rReader.seek(filePointer);
			readByte = rReader.readByte();
			if (readByte == 0xA) {
				if (filePointer != fileLength) {
					posReached = true;
					rReader.seek(filePointer);
				}
			} else if (readByte == 0xD && filePointer != fileLength - 1) {
				posReached = true;
				rReader.seek(filePointer);
			}

			filePointer--;
		}
	}

	public String readLine() throws IOException {
		return rReader.readLine();
	}

	public long getLastAppendTime() {
		return lastAppendTime;
	}

	public void setLastAppendTime(long lastAppendTime) {
		this.lastAppendTime = lastAppendTime;
	}

	public boolean appendLine(String buffer) {
		boolean ret = bufferList.add(buffer);
		if (ret) {
			lastAppendTime = System.currentTimeMillis();
		}

		return ret;
	}

	public int getLineSize() {
		return bufferList.size();
	}

	public StringBuffer getAllLines() {

		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < bufferList.size(); i++) {
			sb.append(bufferList.get(i));
		}
		return sb;
	}

	public void setHeader(String key, String value) {
		headers.put(key, value);
	}

	public String getHeader(String key) {
		headers.get(key);
		return null;
	}

	public void clear() {
		bufferList.clear();
		headers.clear();
	}

	public boolean isFileIsOpen() {
		return fileIsOpen;
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	public List<String> getBufferList() {
		return bufferList;
	}

	public void setBufferList(List<String> bufferList) {
		this.bufferList = bufferList;
	}

	public Transaction getTransaction() {
		return transaction;
	}

	public void setTransaction(Transaction transaction) {
		this.transaction = transaction;
	}

	public BufferedReader getBufferedReader() {
		return bufferedReader;
	}

	public void setBufferedReader(BufferedReader bufferedReader) {
		this.bufferedReader = bufferedReader;
	}

	public void close() throws IOException {
		rReader.close();
		fileIsOpen=false;
	}
	
	public void open() throws IOException {
		rReader = new RandomAccessFile(file, "r"); 
		seekToLastLine(rReader);
		fileIsOpen=true;
	}

	public Path getFilePath() {
		return filePath;
	}
	
	public void setFilePath(Path path) {
		filePath=path;
		file = new File(path.toString());
	}
}
