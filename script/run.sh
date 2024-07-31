java -cp ../target/savepointViewer-1.0-SNAPSHOT.jar \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
org.stateview.StateViewerCLI $1 $2 $3