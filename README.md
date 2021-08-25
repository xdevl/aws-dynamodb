# Node DynamoDB DAO

A library to easily integrate with DynamoDB. This library provides the following features out of the box:
* Simple and type safe API
* Split bulk inserts into chunks of no more than 25 items
* Seamlessly handle query result pagination 
* Handle Dynamo DB reserved words
* Single table design

## Installation
You can install the library using npm:
``` shell
npm install node-dynamodb-dao
```
The use of typescript with strict mode is also highly recommended for which this library has full typing support.


## Usage
This library has been designed with modularity in mind to give the ability to choose the right level of abstraction to use for a project in order to facilitate integration with existing code base.

Most of the APIs make use of ES6 generators rather than arrays or lists in order to efficiently handle large number of items hence the use of the `generator` and `collect` functions you will see in the code samples allowing to easily turn a list of objects into a generator and vice-versa.


## Serialization API
This is the core module of this library, it allows you to easily define a serializer in order to parse and generate DynamoDB encoded values. The API makes use of typescript mappings rather than the usual, widely used, annotations in order to fully decouple the serialization definition from your project classes. This is effectively a bit more verbose but gives you more flexibility and the API has strong typing support which means you will necessarily get compiler errors if your serializer isn't compatible with your mapped class (ie: type mismatch or missing / extra fields)

### Overview
From the following class for example:
``` typescript
class User {
    public readonly surname: string;
    public readonly firstName: string;
    public readonly gender: Gender;
    public readonly salary: number;
    public readonly dateOfBirth: Date;
    public readonly manager?: User;
    public readonly roles: string[];
}
```

One can easily build a Dynamo DB serializer as below.
Note the use of the `typeOf` function allowing us to pass our serialized type as a parameter, not to get confused with the regular `typeof` javascript  operator. The `self` parameter can also be used to reference the serializer being currently defined.
``` typescript
const userSerializer = new DynamoSerializer(typeOf<User>(), (self) => ({
    surname: DynamoSerializer.string(),
    firstName: DynamoSerializer.string(),
    gender: DynamoSerializer.string<Gender>(),
    salary: DynamoSerializer.number(),
    dateOfBirth: DynamoSerializer.date(),
    manager: DynamoSerializer.optional(self),
    roles: DynamoSerializer.list(DynamoSerializer.string())
}), (attrs) => new User(attrs));
```
Dynamo DB values can then easily be serialized or deserialized:
``` typescript
const sarah = new User(...)
const dynamoDBValue = userSerializer.serialize(sarah);
const userValue = userSerializer.deserialize(dynamoDBValue);
```

### Custom serializer

If the predefined serializers are not enough, you can define your own. Below is and example of a custom date serializer that uses a string rather than a number:
``` typescript
const myDateSerializer: IDynamoSerializer<Date, "S"> = {
    type: "S",
    deserialize: (value) => new Date(DynamoSerializer.string().deserialize(value)),
    serialize: (value) => DynamoSerializer.string().serialize(value.toString()),
}
```

## Data Access Object API
The DAO module builds up on the serialization module and provide the main point of interaction with your DynamoDB table. DynamoDB supports primary keys and composite keys when creating tables or global indexes, for methods that take a key specification as a parameter you'll be able to either pass:
* A simple string for a primary key
* A pair of string where the first one is the partition key and the second one the sort key

### Overview
First of all you'll need to define your table structure giving a DynamoDB instance, a table name, the previously defined serializer and the table key specification:
``` typescript
import {DynamoDB} from "aws-sdk";
import {DynamoDao, collect, generator} from "node-dynamodb-dao";

const dynamoDb = new DynamoDB("http://localhost:8000");
const dao = new DynamoDao(dynmoDb, "UserTable", userSerializer, ["surname", "firstName"]);
```

The DAO class exposes a handy function you can call to then automatically creates the DynamoDB table if it does not exists:
``` typescript
await dao.createTableIfNeeded({
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1,
});
```

You can then start persisting values:
```typescript
await dao.persist(generator(new User(...)));
```

And reading them back:
``` typescript
// list all users
await collect(dao.list());
// retrieve a specific user
await dao.get(["smith", "john"]);
```

To remove values, simply use the delete method passing a key value:
```typescript
await dao.delete(["howard", "louis"]);
```

### Querying

For tables with a composite key, querying can be done on the sort key for a given partition key as per DynamoDB capabilities:

``` typescript
// list all users with a surname of dupont
await collect(dao.lookup("dupont"));
// list all users with a surname of dupont and a first name starting with "a"
await collect(dao.lookup("dupont", {
    condition: {
        matcher: "begins_with",
        value: "a"
    }
}));
```

There isn't any dedicated API for Query filters yet but you can still use them should you need to by overwriting the request generated by the library, you just won't get any typing verification support:
``` typescript
// list all users and filter out the ones with a salary below 20K
await collect(dao.list({
    overwrite: (scanInput) => {
        scanInput.FilterExpression = "salary > :salary";
        scanInput.ExpressionAttributeValues = Object.assign(scanInput.ExpressionAttributeValues ?? {}, {
            ":salary": DynamoSerializer.number().serialize(20 * 1000)
        });
        return scanInput;
    }
}));
```

Paging can be achieved using the options of the `list` and `lookup` methods:
``` typescript
var startFrom = undefined;
do {
    await collect(dao.list({
        limit: 10,
        startFrom: startFrom,
        onMore: (token) => {
            startFrom = token;
        }
    }));
} while(startFrom);

```

### Indexes

Indexes can be created from a DAO instance as such:
``` typescript
// local index, all properties are projected
const birthLocalIndex = dao.localIndex("birthIndex", "dateOfBirth");
// global index, only the dateOfBirth property is projected along with the table and index key properties
const salaryGlobalIndex = dao.globalIndex("salaryIndex", "salary", ["dateOfBirth"]);
```
When creating indexes, the last parameter is an optional projection which specifies what properties you'll need for the index. If none is being passed it is defaulted to all properties. 


Note that local and global indexes needs to be explicitly specified when creating the DynamoDB table and global indexes will use the same provisioning capacity than the table.
``` typescript
await dao.createTableIfNeeded({
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1,
}, birthLocalIndex, salaryGlobalIndex);
```

Indexes can be queried in the same way that DAOs are:
``` typescript
// list all users named dupont born between 1990 and 2000
await collect(localIndex.lookup("dupont", {
    condition: {
        matcher: "between",
        value: [new Date("01/01/1990").getTime(), new Date("31/12/2000").getTime()]
    }
}));
// list users by descending order of salary
await collect(salaryGlobalIndex.list({
    overwrite: (scanInput) => ({...scanInput, ScanIndexForward: false});
}));
```


## Entity API
The Entity module is an abstraction of the DAO module which is aimed at simple applications that use a single table in order to store all their data. Depending on your use case, this may not be the most performant option but it is definitely a good starting point if you don't want to have to deal with complex deployments especially if your application doesn't necessarily require it.

This API assumes a rather simplistic access usage which can either be done by unique identifier or lookup on one or a combination of several properties of your choice. As such, creating an `EntityDao` takes the following parameters:
* A DynamoDB object
* The name of the DynamoDB table to use to persist entities
* The serializer to use to serialize an deserialize entities
* The name of the entities you want to persist using this dao
* A function returning the unique identifier of an entity as a string
* A function returning a value that can be used to lookup your entities other than with their unique identifier

``` typescript
import {DynamoDB} from "aws-sdk";
import {EntityDao, collect, generator} from "node-dynamodb-dao";

const dynamoDb = new DynamoDB("http://localhost:8000");
const entityDao = new EntityDao(
    dynamoDb,
    "my-application-table",
    userSerializer,
    "User",
    (user) => `${user.surname}|${user.firstName}`,
    (user) => `${user.manager?.surname}|${user.gender}`);
```

Before being able to run any queries, you'll need to make sure the DynamoDB table used to store your entities is created if not already: 
``` typescript
await EntityDao.synchronise(dynamoDb, "my-application-table", {
    ReadCapacityUnits: 1,
    WriteCapacityUnits: 1,
})
```

You can then start persisting entities:
``` typescript
await entityDao.persist(generator(new User(...)))
```



And reading them back, the optional lookup parameter matches content using the `begins_with` matcher in order to be able to match has many properties as you need:

``` typescript
// list the first 10 users
await collect(entityDao.list({limit: 10}));
// list all users who have got Smith as a manager 
await collect(entityDao.list({lookup: "smith|"}));
// list all women who have got Smith as a manager 
await collect(entityDao.list({lookup: "smith|female"}));
// find a user by his unique identifier
await collect(entityDao.get("sarah|connor"));
```

There isn't any dedicated API for Query filters yet but you can still use them should you need to by overwriting the request generated by the library, you just won't get any typing verification support:

``` typescript
// list all users and filter out women
await collect(entityDao.list({
        overwrite: (queryInput) => {
            queryInput.FilterExpression = "ENTITY_VALUE.gender = :gender";
            queryInput.ExpressionAttributeValues = Object.assign(queryInput.ExpressionAttributeValues ?? {}, {
                ":gender": DynamoSerializer.string().serialize(Gender.MALE)
            });
            return queryInput;
        }
    }));
```

To remove entities, simply use the delete method passing the entity unique identifier:
```typescript
await entityDao.delete("sarah|connor");
```

## License
This software is licensed under the [MIT license](LICENSE)

Copyright &#169; 2021 All rights reserved. XdevL