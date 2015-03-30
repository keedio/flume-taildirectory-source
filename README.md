flume-taildirectory-source
===========================
Source of Flume NG for tailing files in a directory

Notes
=====
This plugin is based on jinoos (jinoos@gmail.com) https://github.com/jinoos/flume-ng-extends  
Is refactored to support logs rotate in windows and linux, and the code has been cleaned to much more simple working, and apache.common.vfs2 dependency has been replaced with the native java 7 java.nio library.  
Thanks for the inspiration.

Compilation
===========
```
mvn package
```

Use
===
Make the directory in flume installation path ```$FLUME_HOME/plugins.d/tail-directory-source/lib``` and copy the file   ```flume-taildirectory-source-1.1.1.jar``` in it.  
Edit flume configuration file with the parameters above.

Configuration
=============
| Property Name | Default | Description |
| ------------- | :-----: | :---------- |
| Channels | - |  |
| Type | - | org.apache.flume.source.taildirectory.DirectoryTailSource |
| dirs | - | NICK of directories, it's such as list of what directories are monitored |
| dirs.NICK.path | - | Directory path |
| unlockFileTime | 1 | Delay to check not modified files to unlock the access to them ( in minutes ) |
| fileHeader | false | Include file absolute path in events header |
| fileHeaderKey | file | Key of file absolute path header |
| basenameHeader | false | Include file base name in events header |
| basenameHeaderKey | basename | Key of file base name header |


* Example
```
agent.sources = tailDir
agent.sources.tailDir.type = org.apache.flume.source.taildirectory.DirectoryTailSource
agent.sources.tailDir.dirs = monitDir1 monitDir2
agent.sources.tailDir.dirs.monitDir1.path = /var/lib/flume/tailDir-1
agent.sources.tailDir.dirs.monitDir2.path = /var/lib/flume/tailDir-2
agent.sources.tailDir.dirs.unlockFileTime = 1
agent.sources.tailDir.basenameHeader = true
agent.sources.tailDir.basenameHeaderKey = basenameFilename
agent.sources.tailDir.fileHeader = true
agent.sources.tailDir.fileHeaderKey = file
agent.sources.tailDir.followLinks = false

agent.sources.tailDir.channels = memoryChannel
```
