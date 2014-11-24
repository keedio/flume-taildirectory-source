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
Make the directory in flume installation path ```$FLUME_HOME/plugins.d/tail-directory-source/lib``` and copy the file   ```flume-taildirectory-source-0.0.1.jar``` in it.  
Edit flume configuration file with the parameters above.

Configuration
=============
| Property Name | Default | Description |
| ------------- | :-----: | :---------- |
| Channels | - |  |
| Type | - | org.apache.flume.source.taildirectory.DirectoryTailSource |
| dirs | - | Nick of directories, it's such as list of what directories are monitored |
| dirs.NICK.path | - | Directory path |

* Example
```
agent.sources = tailDir
agent.sources.tailDir.type = org.apache.flume.source.taildirectory.DirectoryTailSource
agent.sources.tailDir.dirs = tmpDir varLogDir
agent.sources.tailDir.dirs.tmpDir.path = /tmp
agent.sources.dirMon.dirs.varLogDir.path = /var/log
```
