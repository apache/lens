Apache Lens
=====

Apache Lens is a unified Analytics Platform. Lens aims to cut the Data Analytics silos by providing a single view of data
across multiple tiered data stores and optimal execution environment for the analytical query.

Prerequisites :
Apache Lens requires JDK(>=1.7) and Apache Maven(3.x) to be installed for the build.

JAVA_HOME is required for running tests.

Confirm versions :
  # java -version
  # mvn --version
  # echo ${JAVA_HOME}

Additionally MAVEN_OPTS can be configured as :
  # export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=256m"

Build the project :
  # mvn clean package


See [Development Environment Setup] (http://lens.apache.org/developer/contribute.html#Development_Environment_Setup)
and [Building from source] (http://lens.apache.org/developer/contribute.html#Building_from_source) docs for
more details.

[Detailed documentation for the project is available here](https://lens.apache.org)
