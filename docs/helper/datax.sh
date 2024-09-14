source ~/.bash_profile
cd /home/hadoop/IdeaProjects/Exchangis

#linkis-engineplugin-datax
#mvn -Dmaven.test.skip=true package -pl exchangis-engines/engineconn-plugins/datax
#find $LINKIS_HOME/../ -name linkis-engineplugin-datax-1.1.2.jar  -print -exec  cp exchangis-engines/engineconn-plugins/datax/target/linkis-engineplugin-datax-1.1.2.jar  {} \;

#exchangis-job-server-
#mvn -Dmaven.test.skip=true package -pl exchangis-job/exchangis-job-server
#find $LINKIS_HOME/../ -name exchangis-job-server-1.1.2.jar  -print -exec  cp exchangis-job/exchangis-job-server/target/exchangis-job-server-1.1.2.jar  {} \;

#exchangis-engines/engines/datax/datax-hdfsreader
mvn -Dmaven.test.skip=true package -pl exchangis-engines/engines/datax/datax-hdfsreader
find $LINKIS_HOME/../ -name datax-hdfsreader-3.0.0-Plus-2.jar  -print -exec  cp exchangis-engines/engines/datax/datax-hdfsreader/target/datax-hdfsreader-3.0.0-Plus-2.jar  {} \;


cd $EXCHANGIS_HOME
sh sbin/daemon.sh stop server
sh sbin/daemon.sh restart server