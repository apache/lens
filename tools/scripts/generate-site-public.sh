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

REPO=git@github.com:InMobi/grill.git
TMP=/tmp/lens-site-stage
STAGE=`pwd`/target/staging
REST_DIR=`pwd`/lens-server/target/site/wsdocs
IMAGES_DIR=`pwd`/src/site/apt/figures
LOGO_FILE=`pwd`/grill-logo.png
VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)' || die "unable to get version")


echo "Starting generate-site"
CURR_BRANCH=`git branch | sed -n '/\* /s///p'`
echo "Running site in current lens branch" $CURR_BRANCH
mvn clean test -Dtest=TestGenerateConfigDoc || die "Unable to generate config docs"
mvn install -DskipTests
mvn clean site site:stage -Ddependency.locations.enabled=false -Ddependency.details.enabled=false || die "unable to generate site"
cd lens-server
mvn enunciate:docs
cd ..
echo "Site gen complete"

rm -rf $TMP || die "unable to clear $TMP"

echo "Beginning push to gh-pages from " $CURR_BRANCH
git clone $REPO $TMP || die "unable to clone $TMP"
cd $TMP
git checkout gh-pages || die "unable to checkout gh-pages"

mkdir -p current || die "unable to create dir current"
mkdir -p wsdocs || die "Unable to create dir for REST docs"
mkdir -p versions/$VERSION || due "unable to create dir versions/$VERSION"

find current -type f -exec git rm {} \;
echo "Copying REST docs from " $REST_DIR
cp $LOGO_FILE .
# Delete index.html from the source wsdocs as it conflitcs with maven index.html
echo "DELETE $REST_DIR/index.html"
rm $REST_DIR/index.html
echo "Copy enunciate documentation"
cp -r $REST_DIR/* .
echo "Copy images"
cp -r $IMAGES_DIR . 
echo "Copy MVN site"
cp -r $STAGE/ . || die "unable to copy to base"
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

git add . || die "unable to add for commit"
git commit -m "Updated documentation for version $VERSION. Source branch $CURR_BRANCH" || die "unable to commit to git"
git push origin gh-pages || die "unable to push to gh-pages"

cd $STAGE
rm -rf $TMP || die "unable to clear $TMP"
