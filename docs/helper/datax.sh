source ~/.bash_profile
cd /home/hadoop/IdeaProjects/Exchangis
mvn -Dmaven.test.skip=true package -pl exchangis-engines/engineconn-plugins/datax
find $LINKIS_HOME/../ -name linkis-engineplugin-datax-1.1.2.jar  -print -exec  cp exchangis-engines/engineconn-plugins/datax/target/linkis-engineplugin-datax-1.1.2.jar  {} \;
