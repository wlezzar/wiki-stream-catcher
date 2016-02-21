BASENAME=$(dirname $0)
echo $BASENAME

source $BASENAME/env.sh

export PYTHONPATH=$PROJECT_HOME/stream_catcher
python $PROJECT_HOME/stream_catcher/org/lezzar/wikistream/WikiStreamCatcher.py