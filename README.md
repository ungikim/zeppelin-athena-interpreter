# zeppelin-athena-interpreter

Amazon Athena interpreter for Apache Zeppelin.

## Build

Requirement: Zeppelin must be in your local repo.

```sh
mvn clean package
```

## Deployment

* Update `$ZEPPELIN_HOME/conf/zeppeln-site.xml`
```xml
<property>
  <name>zeppelin.interpreters</name>
  <value>...,org.apache.zeppelin.athena.AthenaInterpreter,org.apache.zeppelin.athena.AthenaDownloadInterpreter</value>
</property>
```
* Create `$ZEPPELIN_HOME/interpreter/athena`
* Copy interpreter jar in `$ZEPPELIN_HOME/interpreter/athena`
