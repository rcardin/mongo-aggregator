[![Build Status](https://travis-ci.org/rcardin/mongo-aggregator.svg?branch=master)](https://travis-ci.org/rcardin/mongo-aggregator)
# mongo-aggregator

## Introduction
This library implements a fluent Java API for MongoDB Aggragation Framework. The
[MongoDB Aggregation framework](https://docs.mongodb.com/v3.4/aggregation/) processes data records and returns computed
result. The library implements the [aggregation pipeline](https://docs.mongodb.com/v3.4/aggregation/#aggregation-pipeline).

## Usage
Actually, the MongoDB Java driver does not support any form of fluent interface to features of the aggregation framework. The `mongo-aggregator` library enables you to easily create an *aggregation pipeline*, in a single Java statement.

For example, take the definition of the following pipeline.

```java
Aggregator
    .of(embeddedMongo.mongoClient().getDatabase(DATABASE))
    .collection(COLLECTION)
    .filter(eq("name", "MongoDB"))
    .projection(project(fields(excludeId(), include("name", "count"))))
    .groupBy(group("$name", new BsonField("total", new BasicDBObject("$sum", "$count"))))
    .execute(documents -> {
        List<Document> list = new ArrayList<>();
        for (Document doc: documents) {
            list.add(doc);
        }
        return list;
    });
```

Giving to the `of` method an instance of `MongoDatabase`, you create an empty pipeline.
Once you have a pipeline, provide through the method `collection` the name of the collection
you want to use.

Well, it's time to populate the pipeline. By now, the library supports only pipelines built of exactly
one filter stage, one projection stage and one group by stage. In future versions we are plan to manage also different kinds of pipelines

The *filter* stage should be provided using the `filter` method. This method accepts a `Bson` object. Use
[`com.mongodb.client.model.Filters`](http://api.mongodb.com/java/3.3/?com/mongodb/client/model/Filters.html)
class and its methods to create a valid filters.

Likewise, the *projection* stage should be provided by the `projection` method. Also this method accept a `Bson`
object as input. To create a valid projection stage, please use the classes [`com.mongodb.client.model.Aggregates`](http://api.mongodb.com/java/3.3/?com/mongodb/client/model/Aggregates.html)
and [`com.mongodb.client.model.Projections`](http://api.mongodb.com/java/3.3/?com/mongodb/client/model/Projections.html) and their relative methods.

Finally the *group by* stage should be provided by the `groupBy` method. Refer to the aggregation framework documentation
to understand the right structure of the document to pass to the method.

To consume the result of the aggregation pipeline, provide a proper function to the `execute` method. The function should
transform an `AggregateIterable<Document>`, which is the result of the execution of the aggregagtion pipeline, in another type of
interest.

By now, the library uses the synchronous version of the MongoDB Java driver. Future versions will support also the
asynchronous version of the driver.

## Installation
The library is built using Maven 3. To build the archive containing the library, simply type the following command:

```
mvn clean install
```

To execute only provided tests, type instead

```
mvn test
```
