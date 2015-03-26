package org.apache.flume.source.taildirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileKeys {
	
	private static final Logger LOGGER= LoggerFactory
			.getLogger(FileKeys.class);
	
	private FileKeys(){
	}
	
	public static String getFileKey(Path path){
		if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0)
			return path.toString();
		else
			try{
				return Files.readAttributes(path, BasicFileAttributes.class)
						.fileKey().toString();
			}catch (IOException e){
				LOGGER.warn(e.getMessage(),e);
				LOGGER.warn("File {} not found, maby removed/moved",path);
				return null;
			}
	}
}
