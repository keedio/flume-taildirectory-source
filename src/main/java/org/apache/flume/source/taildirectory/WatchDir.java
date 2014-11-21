package org.apache.flume.source.taildirectory;

import java.nio.file.*;
import java.nio.file.WatchEvent.Kind;

import static java.nio.file.StandardWatchEventKinds.*;
import static java.nio.file.LinkOption.*;

import java.nio.file.attribute.*;
import java.io.*;
import java.util.*;

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
    //private static Hashtable<String, FileSet> fileSetMap;
    private AbstractSource source;
    private Hashtable<String, FileSet> fileSetMap;
    
    private static final Logger logger = LoggerFactory.getLogger(WatchDir.class);
    private String OS;
 
    @SuppressWarnings("unchecked")
    static <T> WatchEvent<T> cast(WatchEvent<?> event) {
        return (WatchEvent<T>)event;
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
 
    /**
     * Creates a WatchService and registers the given directory
     */
    WatchDir(Path dir, AbstractSource source) throws IOException {
    	
    	logger.debug("WatchDir: WatchDir");
    	
        this.watcher = FileSystems.getDefault().newWatchService();
        this.keys = new HashMap<WatchKey,Path>();
        
        OS = System.getProperty("os.name").toLowerCase();
        
        if ( OS.indexOf("win") >= 0)
        	OS = "win";
        else if ( OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0)
        	OS = "unix";
        else if ( OS.indexOf("mac") >= 0 )
        	OS = "mac";
        else if ( OS.indexOf("sunos") >= 0 )
        	OS = "solaris";

        	
        this.fileSetMap = new Hashtable<String,FileSet>();
        this.source = source;
 
        logger.info("Scanning " + dir);
        registerAll(dir);
        logger.info("Done.");
    }
    
    /**
     * Process all events for keys queued to the watcher
     */
    void processEvents() {
    	
    	logger.debug("WatchDir: processEvents");
    	
        for (;;) {
 
            // wait for key to be signalled
            WatchKey key;
            try {
                key = watcher.take();
            } catch (InterruptedException x) {
                return;
            }
 
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
                
                try{
	                if (kind == ENTRY_CREATE){
	                	fileCreated(path);
	                }
	                else if (kind == ENTRY_MODIFY){
	                	fileModified(path);
	                }
	                else if (kind == ENTRY_DELETE){
	                	fileDeleted(path);
	                }
                } catch (IOException x) {
                    x.printStackTrace();
                }
            }

 
            // reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);
                // all directories are inaccessible
                if (keys.isEmpty()) {
                    break;
                }
            }
        }
    }
    
    private void fileCreated(Path path) throws IOException{
    	
    	logger.debug("WatchDir: fileCreated");
    	
    	if (Files.isDirectory(path, NOFOLLOW_LINKS))
            registerAll(path);
    	else{
    		addFileSetToMap(path); 		
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
		String key;
		
		if ( OS == "win")
			key = path.toString();
		else if ( OS == "unix" || OS == "mac " || OS == "mac " || OS == "solaris ")
			key = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
		else
			throw new IOException("OS unknown");
		
		if (fileSetMap.containsKey(key)) 
			fileSet = fileSetMap.get(key);
		else
			fileSet = addFileSetToMap(path);
		
		return fileSet;
	}
    
    private FileSet addFileSetToMap(Path path) throws IOException{
    	logger.debug("WatchDir: addFileSetToMap");
    	String key;
    	FileSet fileSet;
    	
		if ( OS == "win")
			key = path.toString();
		else if ( OS == "unix" || OS == "mac " || OS == "mac " || OS == "solaris ")
			key = Files.readAttributes(path, BasicFileAttributes.class).fileKey().toString();
		else
			throw new IOException("OS unknown");
		
		if (!fileSetMap.containsKey(key)) {
			fileSet = new FileSet(path.toString());
			fileSetMap.put(key, fileSet);
		}
		else
			fileSet = fileSetMap.get(key);

		return fileSet;
    }

	private void fileDeleted(Path path){
    	
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
}