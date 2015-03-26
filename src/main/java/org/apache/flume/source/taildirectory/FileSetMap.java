package org.apache.flume.source.taildirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSetMap extends HashMap<String, FileSet> {
	
	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory
			.getLogger(FileSetMap.class);
	
	private HashMap<String, String> filePathsAndKeys;
	
	FileSetMap(HashMap<String, String> filePathsAndKeys){
		super();
		this.filePathsAndKeys = filePathsAndKeys;
	}
	
	public FileSet getFileSet(Path path) throws IOException {

		String fileKey = FileKeys.getFileKey(path);

		if (this.containsKey(fileKey)) {
			return this.get(fileKey);
		} else {
			return addFileSetToMap(path, "lastLine");
		}
	}
	
	public FileSet addFileSetToMap(Path path, String startFrom)
			throws IOException {

		FileSet fileSet;
		String fileKey = FileKeys.getFileKey(path);

		if (!this.containsKey(fileKey)) {
			logger.info("Scanning file: " + path.toString() + " with key: "
					+ fileKey);
				fileSet = new FileSet(path, startFrom);
				filePathsAndKeys.put(path.toString(), fileKey);
				this.put(fileKey, fileSet);
		} else{
			fileSet = this.get(fileKey);
			
			if (!fileSet.getFilePath().toString().equals(path.toString())){
				fileSet.setFilePath(path);
			}
			/*
			if (!fileSet.isFileIsOpen()){
				fileSet.open();
			}*/
		}
		return fileSet;
	}
}
