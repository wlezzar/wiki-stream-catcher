BASENAME=$(dirname $0)
echo $BASENAME

source $BASENAME/env.sh

cd $PROJECT_HOME/wiki_stream_handler
mvn clean package
cd -
