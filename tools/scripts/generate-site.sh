#!/bin/sh

die() {
    echo "$1"
    exit 1
}

REPO=https://github.corp.inmobi.com/platform/grill.git
TMP=/tmp/grill-site-stage
STAGE=`pwd`/target/staging
VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version|grep -Ev '(^\[|Download\w+:)' || die "unable to get version")


echo "Starting generate-site"
CURR_BRANCH=`git branch | sed -n '/\* /s///p'`
echo "Running site in current grill branch" $CURR_BRANCH
mvn clean test -Dtest=TestGenerateConfigDoc || die "Unable to generate config docs"
mvn clean site site:stage -Ddependency.locations.enabled=false -Ddependency.details.enabled=false || die "unable to generate site"
echo "Site gen complete"

rm -rf $TMP || die "unable to clear $TMP"

echo "Beginning push to gh-pages from " $CURR_BRANCH
git clone $REPO $TMP || die "unable to clone $TMP"
cd $TMP
git checkout gh-pages || die "unable to checkout gh-pages"

mkdir -p current || die "unable to create dir current"
mkdir -p versions/$VERSION || due "unable to create dir versions/$VERSION"

find current -type f -exec git rm {} \;

cp -r $STAGE/ . || die "unable to copy to base"
cp -r $STAGE/ current/ || die "unable to copy to current"
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
