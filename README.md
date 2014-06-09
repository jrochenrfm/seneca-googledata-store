#seneca-googledata-store

###Seneca node.js data-storage plugin for the Google Cloud Datastore

This module is a plugin for the Seneca framework. It provides a storage engine that uses Google Cloud Datastore to persist data.

The Seneca framework provides an [ActiveRecord-style data storage API](http://senecajs.org/data-entities.html). Each supported database has a plugin, such as this one, that provides the underlying Seneca plugin actions required for data persistence.


###Quick example
```javascript
var seneca    = require('seneca')();
var options = {
			    'DATASTORE_SERVICE_ACCOUNT': '123456789@developer.gserviceaccount.com',
			    'DATASTORE_PRIVATE_KEY_FILE': 'path/to/example.pem',
			    'SERVICE_ACCOUNT_SCOPE': [scope1,scope2],
			    'DATASETID': 'delta-charley-tango-101'
			    }


seneca.use('googledata-store', options);
```

###Install
	npm install seneca
	npm install seneca-googledata-store


###Usage

You don't use this module directly. It provides an underlying data storage engine for the Seneca entity API:

```javascript
var entity = seneca.make$('typename')
entity.someproperty = "something"
entity.anotherproperty = 100

entity.save$( function(err,entity){ ... } )
entity.load$( {id: ...}, function(err,entity){ ... } )
entity.list$( {property: ...}, function(err,entity){ ... } )
entity.remove$( {id: ...}, function(err,entity){ ... } )
```

###Queries

The standard Seneca query format is supported:

- `entity.list$({field1:value1, field2:value2, ...})` implies pseudo-query field1==value1 AND field2==value2, ...
- you can only do AND queries. 
- `entity.list$({f1:v1,...},{sort$:{field1:1}})` means sort by field1, ascending
- `entity.list$({f1:v1,...},{sort$:{field1:-1}})` means sort by field1, descending
- `entity.list$({f1:v1,...},{limit$:10})` means only return 10 results
- `entity.list$({f1:v1,...},{skip$:5})` means skip the first 5
- `entity.list$({f1:v1,...},{fields$:['field1','field2']})` means only return the listed fields (avoids pulling lots of data out of the database)
- you can use sort$, limit$, skip$ and fields$ together


###Native Driver

As with all seneca stores, you can access the native driver, in this case, the Google Cloud datastore object using `entity.native$(function(err, datastore){...})`.

With the datastore object you can perform any [JSON API](https://developers.google.com/datastore/docs/apis/v1beta2/) calls including building a [GQL Query](https://developers.google.com/datastore/docs/concepts/gql)

```javascript
entity.native$(function(err, datastore){
	datastore.runQuery({
    gqlQuery: {
      queryString: 'SELECT * FROM Person',
    }
  }).execute(function(err, result) {
    if (!err) {
      // Iterate over the results and return the entities.
      result = (result.batch.entityResults || []).map(
        function(entityResult) {
          return entityResult.entity;
        });
    }
  });
})
```

###Test
	cd test

	mocha googledata-store.test.js --seneca.log.print