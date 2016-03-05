BASENAME=$(dirname $0)
echo $BASENAME

source $BASENAME/env.sh

cd $PROJECT_HOME/WikiStreamHandler
mvn clean package
cd -
