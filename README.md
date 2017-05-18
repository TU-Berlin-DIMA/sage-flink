# Apache Flink SAGE

## Building Apache Flink SAGE from Source

Prerequisites for building Flink SAGE:

* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (we recommend version 3.0.4)
* Java 7 or 8
* Java Clovis API installed in local Maven directory (see [ClovisSDK Repo](https://bitbucket.org/mnoumanshahzad/clovissdk))

```
git clone -b sage-dev https://mnoumanshahzad@bitbucket.org/lutzcle/flink-sage.git
cd flink-sage
mvn clean package -DskipTests # this will take up to 10 minutes
```

You might also want to install the flink-sage connector to your local maven repo
In order to that, run the following commands

```
cd flink-sage/flink-connectors/flink-sage
mvn clean install
```

Flink is now installed in `build-target`

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.0.3 creates the libraries properly.
To build unit tests with Java 8, use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [http://plugins.jetbrains.com/plugin/?id=1347](http://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://github.com/apache/flink/blob/master/docs/internals/ide_setup.md#intellij-idea) guide for details.

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.

