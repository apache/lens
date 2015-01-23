#!/bin/sh
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


die() {
    echo "$1"
    exit 1
}

SVN_TARGET=$1
TMP=/tmp/lens-site-stage
STAGE=`pwd`/target/staging
REST_DIR=`pwd`/lens-server/target/site/wsdocs
VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)' || die "unable to get version")


echo "Starting generate-site"
CURR_BRANCH=`git branch | sed -n '/\* /s///p'`
echo "Running site in current lens branch" $CURR_BRANCH
mvn clean test -Dtest=TestGenerateConfigDoc || die "Unable to generate config docs"
mvn install -DskipTests
mvn site site:stage -Ddependency.locations.enabled=false -Ddependency.details.enabled=false || die "unable to generate site"
cd lens-server
mvn enunciate:docs
cd ..
echo "Site gen complete"

rm -rf $TMP || die "unable to clear $TMP"
mkdir -p $TMP

cd $TMP

mkdir -p current || die "unable to create dir current"
mkdir -p versions/$VERSION || due "unable to create dir versions/$VERSION"

find current -type f -exec git rm {} \;
echo "Copying REST docs from " $REST_DIR
# Delete index.html from the source wsdocs as it conflitcs with maven index.html
echo "DELETE $REST_DIR/index.html"
rm $REST_DIR/index.html
echo "Copy enunciate documentation"
cp -r $REST_DIR/* .
cp -r $REST_DIR/* current/ || die "unable to copy REST to current"
cp -r $REST_DIR/* versions/$VERSION/ || die "unable to copy REST to versions/$VERSION"
echo "Copy MVN site"
cp -r $STAGE/ .
echo "Copy docs to current/"
cp -r $STAGE/ current/ || die "unable to copy to current"
echo "Copy docs to version:" $VERSION
cp -r $STAGE/ versions/$VERSION/ || die "unable to copy to versions/$VERSION"

FILES=$(cd versions; ls -t | grep -v index.html)
echo '<ul>' > versions/index.html
for f in $FILES
do
    echo "<li><a href='$f/index.html'>$f</a></li>" >> versions/index.html
done
echo '</ul>' >> versions/index.html


## Copy entire doc directory to Apache SVN Target dir
mkdir -p $SVN_TARGET/site/publish
cp -r $TMP/ $SVN_TARGET/site/publish
cd $SVN_TARGET
echo "Generated site."
