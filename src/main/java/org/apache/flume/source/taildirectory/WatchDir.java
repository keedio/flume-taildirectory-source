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
 * Example to watch a directory (or tree) for changes to files.
 * @param <K>
 */
 
public class WatchDir {
 
    private final WatchService watcher;
    private final Map<WatchKey,Path> keys;
    private AbstractSource source;
    private HashMap<String, FileSet> fileSetMap;
    private HashMap<String, String> filePathsAndKeys;
    private long timeToUnlockFile;
    
    private static final Logger logger = LoggerFactory.getLogger(WatchDir.class);
    private String OS;
	
	private final ScheduledExecutorService scheduler =
		       Executors.newScheduledThreadPool(1);
 
    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>)event;
    }
    
    /**
     * Creates a WatchService and registers the given directory
     */
    WatchDir(Path dir, AbstractSource source, long timeToUnlockFile) throws IOException {
    	
    	this.timeToUnlockFile = timeToUnlockFile;
    	
    	logger.debug("WatchDir: WatchDir");
    	
        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey,Path>();
        
        
        String osName = System.getProperty("os.name").toLowerCase();
        
        if ( osName.indexOf("win") >= 0)
        	OS = "win";
        else if ( osName.indexOf("nix") >= 0 || osName.indexOf("nux") >= 0 || osName.indexOf("aix") > 0)
        	OS = "unix";
        else if ( OS.indexOf("mac") >= 0 )
        	OS = "mac";
        else if ( OS.indexOf("sunos") >= 0 )
        	OS = "solaris";
        else
        	OS = "unknown";

        
        this.filePathsAndKeys = new HashMap<String,String>();
        this.fileSetMap = new HashMap<String,FileSet>();
        this.source = source;
 
        logger.info("Scanning directory: " + dir);
        registerAll(dir);
        logger.info("Done.");
        
    	final Runnable lastAppend = new CheckLastTimeModified();
        scheduler.scheduleAtFixedRate(lastAppend, 0, 1, TimeUnit.MINUTES);
        
    }

	/**
     * Register the given directory with the WatchService
     */
    private void register(Path dir) throws IOException {
    	
    	logger.debug("WatchDir: register");
    	
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        Path prev = keys.get(key);
        logger.info("prev: " + prev);
        if (prev == null) {
            logger.info("register: " + dir);            
        } else {
            if (!dir.equals(prev)) {
                logger.info("update: " +  "-> " + prev + " " + dir);
            }
        }
        
        keys.put(key, dir);
        
        File folder = dir.toFile();
        
        for (final File fileEntry : folder.listFiles()) {
            if (!fileEntry.isDirectory()) {
            	addFileSetToMap(fileEntry.toPath(),"end");                    
            } else {
            	logger.warn("FileEntry found as directory --> TODO: debug this case");
            }
        }
    }
 
    /**
     * Register the given directory, and all its sub-directories, with the
     * WatchService.
     */
    private void registerAll(final Path start) throws IOException {
    	
    	logger.debug("WatchDir: registerAll");
    	
        // register directory and sub-directories
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException
            {
                register(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
 
    private void fileCreated(Path path) throws IOException, InterruptedException{
    	
    	logger.debug("WatchDir: fileCreated");
    	
    	if (Files.isDirectory(path, NOFOLLOW_LINKS))
            registerAll(path);
    	else{
    		addFileSetToMap(path,"begin"); 		
    	}
    }
    
    private void fileModified(Path path) throws IOException{
    	
    	logger.debug("WatchDir: fileModified");
    	
    	String buffer;
    	FileSet fileSet = getFileSet(path);
    	
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
    
    private FileSet getFileSet(Path path) throws IOException {
    	
    	logger.debug("WatchDir: getFileSet");
    	
    	FileSet fileSet;
		String fileKey;
		
		if ( OS == "win")
			fileKey = path.toString();
		else
			fileKey = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
		
		if (fileSetMap.containsKey(fileKey)) {
			fileSet = fileSetMap.get(fileKey);
		}
		else{
			fileSet = addFileSetToMap(path,"lastLine");
		}
			
		
		return fileSet;
	}
    
    private FileSet addFileSetToMap(Path path, String startFrom) throws IOException{
    	
    	logger.debug("WatchDir: addFileSetToMap");
    	
    	String fileKey;
    	FileSet fileSet;
    	
		if ( OS == "win")
			fileKey = path.toString();
		else
			fileKey = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
		
		if (!fileSetMap.containsKey(fileKey)) {
			logger.info("Scanning file: " + path.toString() + " with key: " + fileKey);
			
			synchronized (fileSetMap){
				fileSet = new FileSet(path,startFrom);
				filePathsAndKeys.put(path.toString(), fileKey);
				fileSetMap.put(fileKey, fileSet);
			}
		}
		else
			fileSet = fileSetMap.get(fileKey);

		return fileSet;
    }

	private void fileDeleted(Path path) throws IOException{
		logger.debug("WatchDir: fileDeleted");
		
		String fileKey=null;
		
		if ( OS == "win"){
			fileKey = path.toString();
		}
		else{
			if (filePathsAndKeys.containsKey(path.toString())){
				fileKey = filePathsAndKeys.get(path.toString());
			}
			else
				logger.error("File key of file " + path.toString() + " not found in filePathsAndKeys hashMap");
		}
		
		if (fileKey != null){
			logger.info("Removing file: " + path + " with key: " + fileKey);
			synchronized (fileSetMap){
				if (fileSetMap.containsKey(fileKey)){
					fileSetMap.get(fileKey).clear();
					fileSetMap.get(fileKey).close();
					fileSetMap.remove(fileKey);
				}
			}
			if (filePathsAndKeys.containsKey(path.toString())){
				filePathsAndKeys.remove(path.toString());	
			}
		}
    }
        
	private void sendEvent(FileSet fileSet) {
		
		logger.debug("WatchDir: sendEvent");
		
		if (fileSet.getBufferList().isEmpty())
			return;

		StringBuffer sb = fileSet.getAllLines();
		Event event = EventBuilder.withBody(String.valueOf(sb).getBytes(),
				fileSet.getHeaders());
		source.getChannelProcessor().processEvent(event);

		fileSet.clear();
    }

	public void proccesEvents() {
		logger.debug("WatchDir: run");

		try{
	        for (;;) {
	        	
	        	// wait for key to be signalled
	            WatchKey key;
	            key = watcher.take();
	            Path dir = keys.get(key);
	            
	            if (dir == null) {
	            	logger.error("WatchKey not recognized!!");
	                continue;
	            }
	            
	            for (WatchEvent<?> event: key.pollEvents()) {
	                Kind<?> kind = event.kind();
	
	                // Context for directory entry event is the file name of entry
	                WatchEvent<Path> ev = cast(event);
	                Path name = ev.context();
	                Path path = dir.resolve(name);
	 
	                // print out event
	                logger.debug(event.kind().name() + ": " + path);

	                if (kind == ENTRY_MODIFY){
	                	fileModified(path);
	                }
	                else if (kind == ENTRY_CREATE){
	                	fileCreated(path);
	                }
	                else if (kind == ENTRY_DELETE){
	                	fileDeleted(path);
	                }
	            }
	 
	            // reset key and remove from set if directory no longer accessible
	            boolean valid = key.reset();
	            if (!valid) {
	            	logger.debug("valid?");
	                keys.remove(key);
	                // all directories are inaccessible
	                if (keys.isEmpty()) {
	                    break;
	                }
	            }
	        }
        } catch (IOException x) {
            x.printStackTrace();
        } catch (InterruptedException x) {
        	x.printStackTrace();
            return;
        }
	}
	
    
    public void stop(){
    	
    	logger.debug("WatchDir: stop");
    	try {
	    	for (FileSet fileSet: fileSetMap.values()){
	    		logger.debug("Closing file: " + fileSet.getFilePath());
	    		fileSet.clear();
	    		fileSet.close();
	    	}
    	}catch(IOException x){
    		x.printStackTrace();
	    }
    }
    
    private class CheckLastTimeModified implements Runnable {

    	@Override
    	public void run() {
    		
    		long lastAppendTime, currentTime;
    		String fileKey;
    		
    		
    		try{
	    		
	    		for (FileSet fileSet: fileSetMap.values()){
	    			
	    			lastAppendTime = fileSet.getLastAppendTime();
	    			currentTime = System.currentTimeMillis();
	    			
	    			if ( OS == "win")
	    				fileKey = fileSet.getFilePath().toString();
	    			else
	    				fileKey = Files.readAttributes(fileSet.getFilePath(), 
	    						BasicFileAttributes.class).fileKey().toString();
	    			
	    			logger.debug("Checking file: " + fileSet.getFilePath());
	    			logger.debug("Last append time: " + TimeUnit.MILLISECONDS.toSeconds(
	    					lastAppendTime) + " seconds");
    			
	    			synchronized(fileSetMap){
		    			if (currentTime - lastAppendTime > TimeUnit.MINUTES.toMillis(timeToUnlockFile)){
		    				logger.info("File: " + fileSet.getFilePath() + 
		    						" not modified after " + timeToUnlockFile + " minutes" +
		    						" removing from monitoring list");
		    				fileSetMap.get(fileKey).clear();
		    				fileSetMap.get(fileKey).close();
		    				fileSetMap.remove(fileKey);
		    				filePathsAndKeys.remove(fileSet.getFilePath().toString());
		    			}
	    			}
	    		}
    		}catch (IOException e){
	    		e.printStackTrace();
	    	}
    	}
    }
}