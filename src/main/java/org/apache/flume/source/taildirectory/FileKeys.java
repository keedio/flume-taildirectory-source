package org.apache.flume.source.taildirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

public class FileKeys {
	
	private static String getOSType() {
		String osName = System.getProperty("os.name").toLowerCase();

		if (osName.indexOf("win") >= 0)
			return "win";
		else if (osName.indexOf("nix") >= 0 || osName.indexOf("nux") >= 0
				|| osName.indexOf("aix") > 0)
			return "unix";
		else if (osName.indexOf("mac") >= 0)
			return "mac";
		else if (osName.indexOf("sunos") >= 0)
			return "solaris";
		else
			return "unknown";
	}
	
	public static String getFileKey(Path path){
		if (getOSType() == "win")
			return path.toString();
		else
			try{
				return Files.readAttributes(path, BasicFileAttributes.class)
						.fileKey().toString();
			}catch (IOException e){
				return null;
			}
	}
}
