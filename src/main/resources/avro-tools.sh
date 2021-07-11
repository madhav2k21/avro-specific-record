#Place this in any directory
wget http://central.maven.org/maven2/org/apache/avro/avro-tools/1.8.2/avro-tools-1.8.2.jar
#run this from out project folder
java -jar ~/avro-tools-1.8.2.jar tojosn --pretty customer-generic.avro
java -jar ~/avro-tools-1.8.2.jar tojosn --pretty customer-specific.avro
#getting the schema
java -jar ~/avro-tools-1.8.2.jar getschema customer-specific.avro



