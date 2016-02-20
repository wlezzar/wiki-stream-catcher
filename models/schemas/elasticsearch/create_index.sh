BASENAME=$(dirname $0)

curl -XDELETE localhost:9200/wiki_edits/
curl -XPOST localhost:9200/wiki_edits -d @$BASENAME/wiki_edits.json
