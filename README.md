# nosql2sql
Migrate data from nosql to sql

## current state

Importing a dynamodb export into postgres works. Not all data types are supported yet.

## usage

* update application.yml to set the dynamo, s3, and postgres details
* perform a point in time export for the dynamo table manually.
* When exporting, add a prefix that matches the dynamo table name.
* When export is complete, start the service `./gradlew bootRun`
* Make a POST request to http://localhost:8085/api/import

Note: Swagger-ui is available at http://localhost:8085/swagger-ui.html