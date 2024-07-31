#java -cp ../target/savepointViewer-1.0-SNAPSHOT.jar \
#--add-opens=java.base/java.util=ALL-UNNAMED \
#--add-opens=java.base/java.lang=ALL-UNNAMED \
#--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
#org.stateview.StateViewerCLI view /Users/gezi/Dev/tmp/savepoint-03503c-0b141632f106 -uid source


java -cp ../target/savepointViewer-1.0-SNAPSHOT.jar \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
org.stateview.StateViewerCLI removeAndUpdate /Users/gezi/Dev/tmp/savepoint-03503c-0b141632f106 -uid source
