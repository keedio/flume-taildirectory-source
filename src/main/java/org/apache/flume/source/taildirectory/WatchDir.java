package org.apache.flume.source.taildirectory;

import java.nio.file.*;
import java.nio.file.WatchEvent.Kind;

import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;

import java.nio.file.attribute.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */

public class WatchDir {

	private final WatchService watcher;
	private final Map<WatchKey, Path> keys;
	private AbstractSource source;
	private FileSetMap fileSetMap;
	private HashMap<String, String> filePathsAndKeys;
	private long timeToUnlockFile;

	private static final Logger logger = LoggerFactory
			.getLogger(WatchDir.class);

	private final ScheduledExecutorService scheduler = Executors
			.newScheduledThreadPool(2);

	private DirectoryTailSourceCounter counter;

	@SuppressWarnings("unchecked")
	static <T> WatchEvent<T> cast(WatchEvent<?> event) {
		return (WatchEvent<T>) event;
	}

	/**
	 * Creates a WatchService and registers the given directory
	 */
	WatchDir(Path dir, AbstractSource source, long timeToUnlockFile,
			DirectoryTailSourceCounter counter) throws IOException {

		logger.trace("WatchDir: WatchDir");

		this.timeToUnlockFile = timeToUnlockFile;
		this.counter = counter;

		this.source = source;

		this.watcher = FileSystems.getDefault().newWatchService();
		this.keys = new HashMap<WatchKey, Path>();

		this.filePathsAndKeys = new HashMap<String, String>();
		this.fileSetMap = new FileSetMap(filePathsAndKeys);

		logger.info("Scanning directory: " + dir);
		registerAll(dir);
		
		Thread t = new Thread(new WatchDirRunnable());
		t.start();

		final Runnable lastAppend = new CheckLastTimeModified();
		scheduler.scheduleAtFixedRate(lastAppend, 0, 1, TimeUnit.MINUTES);

		final Runnable printThroughput = new PrintThroughput();
		scheduler.scheduleAtFixedRate(printThroughput, 0, 5, TimeUnit.SECONDS);
	}
	

	/**
	 * Register the given directory, and all its sub-directories, with the
	 * WatchService.
	 */
	private void registerAll(final Path start) throws IOException {

		logger.trace("WatchDir: registerAll");

		// register directory and sub-directories
		Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir,
					BasicFileAttributes attrs) throws IOException {
				register(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * Register the given directory with the WatchService
	 */
	private void register(Path dir) throws IOException {

		logger.trace("WatchDir: register");

		WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE,
				ENTRY_MODIFY);
		Path prev = keys.get(key);

		// TODO: Change this log lines, are not descriptive
		logger.info("prev: " + prev);
		if (prev == null) {
			logger.info("register: " + dir);
		} else {
			if (!dir.equals(prev)) {
				logger.info("update: " + "-> " + prev + " " + dir);
			}
		}

		keys.put(key, dir);

		
		File folder = dir.toFile();

		for (final File fileEntry : folder.listFiles()) {
			if (!fileEntry.isDirectory()) {
				fileSetMap.addFileSetToMap(fileEntry.toPath(), "end");
			} else {
				logger.warn("FileEntry found as directory --> TODO: debug this case");
			}
		}
	}

	private void fileCreated(Path path) throws IOException,
			InterruptedException {

		logger.trace("WatchDir: fileCreated");

		if (Files.isDirectory(path, NOFOLLOW_LINKS))
			registerAll(path);
		else {			
			String buffer;
			FileSet fileSet = fileSetMap.addFileSetToMap(path, "begin");

			if (fileSet.isFileIsOpen()){
				while ((buffer = fileSet.readLine()) != null) {
					if (buffer.length() == 0) {
						logger.debug("Readed empty line");
						continue;
					} else {
						fileSet.appendLine(buffer);
						if (fileSet.getBufferList().isEmpty())
							return;
	
						sendEvent(fileSet);
					}
				}
			}
		}
		
		
		
		
	}

	private void fileModified(Path path) throws IOException {

		logger.trace("WatchDir: fileModified");

		String buffer;
		FileSet fileSet = fileSetMap.getFileSet(path);
		
		if (!fileSet.isFileIsOpen())
			fileSet.open();

		while ((buffer = fileSet.readLine()) != null) {
			if (buffer.length() == 0) {
				logger.debug("Readed empty line");
				continue;
			} else {
				fileSet.appendLine(buffer);
				if (fileSet.getBufferList().isEmpty())
					return;

				sendEvent(fileSet);
			}
		}
	}

	private void fileDeleted(Path path) throws IOException {
		logger.trace("WatchDir: fileDeleted");

		String fileKey = FileKeys.getFileKey(path);
		
		if (fileKey == null){
			fileKey = filePathsAndKeys.get(path.toString());
		}

		if (fileKey != null) {
			FileSet fileSet = fileSetMap.get(fileKey);
			if (fileSet.isFileIsOpen()){
				fileSet.clear();
				fileSet.close();
			}

			if (filePathsAndKeys.containsKey(path.toString())) {
				filePathsAndKeys.remove(path.toString());
			}
		}
	}

	private void sendEvent(FileSet fileSet) {

		logger.trace("WatchDir: sendEvent");

		if (fileSet.getBufferList().isEmpty())
			return;

		StringBuffer sb = fileSet.getAllLines();
		Event event = EventBuilder.withBody(String.valueOf(sb).getBytes(),
				fileSet.getHeaders());
		source.getChannelProcessor().processEvent(event);

		counter.increaseCounterMessageSent();
		fileSet.clear();
	}

	public void stop() {

		logger.trace("WatchDir: stop");
		try {
			for (FileSet fileSet : fileSetMap.values()) {
				logger.debug("Closing file: " + fileSet.getFilePath());
				fileSet.clear();
				fileSet.close();
			}
		} catch (IOException x) {
			logger.error(x.getMessage(),x);
		}
	}

	private class CheckLastTimeModified implements Runnable {

		@Override
		public void run() {

			long lastAppendTime, currentTime;
			FileSet fileSet;

			try {
				Set<String> fileKeySet = new HashSet<String>(
						fileSetMap.keySet());

				for (String fileKey : fileKeySet) {

					fileSet = fileSetMap.get(fileKey);
					if (fileSet.isFileIsOpen()){
						lastAppendTime = fileSet.getLastAppendTime();
						currentTime = System.currentTimeMillis();
						
						logger.trace("FILE: {}",fileSet.getFilePath());
						
						Date expiry = new Date(lastAppendTime);
						logger.trace("LAST APPEND TIME {}", expiry);
						expiry = new Date(currentTime);
						logger.trace("CURRENT TIME {}", currentTime);
						
						logger.debug("Checking file: " + fileSet.getFilePath());
	
						if (currentTime - lastAppendTime > TimeUnit.MINUTES
								.toMillis(timeToUnlockFile)) {
							logger.info("File: " + fileSet.getFilePath()
									+ " not modified after " + timeToUnlockFile
									+ " minutes" + " closing file");
							fileSetMap.get(fileKey).clear();
							fileSetMap.get(fileKey).close();
						}
					}
				}
			} catch (IOException e) {
				logger.error(e.getMessage(),e);
			}
		}
	}

	private class PrintThroughput implements Runnable {

		@Override
		public void run() {
			logger.debug("Current throughput: "
					+ counter.getCurrentThroughput());
		}

	}
	
	private class WatchDirRunnable implements Runnable {

		@Override
		public void run() {
			try {
				for (;;) {
					// wait for key to be signaled
					WatchKey key;
					key = watcher.take();
					Path dir = keys.get(key);

					if (dir == null) {
						logger.error("WatchKey not recognized!!");
						continue;
					}

					for (WatchEvent<?> event : key.pollEvents()) {
						Kind<?> kind = event.kind();

						// Context for directory entry event is the file name of
						// entry
						WatchEvent<Path> ev = cast(event);
						Path name = ev.context();
						Path path = dir.resolve(name);

						// print out event
						logger.trace(event.kind().name() + ": " + path);

						if (kind == ENTRY_MODIFY) {
							fileModified(path);
						} else if (kind == ENTRY_CREATE) {
							fileCreated(path);				
						} else if (kind == ENTRY_DELETE) {
							fileDeleted(path);
						}
					}

					// reset key and remove from set if directory no longer
					// accessible
					boolean valid = key.reset();
					if (!valid) {
						keys.remove(key);
						// all directories are inaccessible
						if (keys.isEmpty()) {
							break;
						}
					}
				}
			} catch (Exception x) {
				logger.error(x.getMessage(), x);
			}
		}

	}
}