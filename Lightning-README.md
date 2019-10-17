# Lightning Enabled Apache Zeppelin

##How to build
./dev/change_scala_version.sh 2.11
mvn clean package -Pbuild-distr  -Pspark-2.4 -Phadoop2.7 -Pscala-2.11 -DskipTests -Dspark.version=2.4.0 -Dcheckstyle.skip=true

package file, zeppelin-ver.tar.gz, will be generated in zeppelin-distribution/target directory

##How to set up Lightning
In Interpreter configuration UI, edit spark interpreter and add the followings :

com.zetaris.lightning.lightningConfFile	: Lightning Configuration file path, by default $LIGHTNING_HOME/conf/lightning-conf.xml
com.zetaris.lightning.metaStoreConfFile	: Lightning meta store property file path, by default $LIGHTNING_HOME/conf/meta_store.properties