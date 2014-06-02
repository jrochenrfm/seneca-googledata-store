/* Copyright (c) 2010-2014 John Roche, MIT License */
"use strict";


var googleapis = require('googleapis');


var name = "google-data-store";


/**
   * params:
   * options - Google DataStore Service Account options
   *  {
   *    'DATASTORE_SERVICE_ACCOUNT': '123456789@developer.gserviceaccount.com',
   *    'DATASTORE_PRIVATE_KEY_FILE': 'path/to/example.pem',
   *    'SERVICE_ACCOUNT_SCOPE': ['scope1', 'scope2'],
   *    'DATASETID': 'delta-charley-tango-101'
   *  }
   */
module.exports = function(options) {
  var seneca = this,
      desc,
      datastore;


/**
  * check and report error conditions seneca.fail will execute the callback
  * in the case of an error.
  */

  function error(args, err, cb) {
    if( err ) {
      seneca.log.error('entity',err,{store:name});
      return true;
    }
    else return false;
  };


function Query(kinds, limit){
  this.query = {
    kinds: kinds,
    filter: {},
    limit: limit
  }

  this.addFilter = function(filter){
    this.query.filter[filter.getFilterType()] = filter.getFilter();
  };
}

function CompositeFilter(operator, filters){
    this.operator = operator;
    this.filters = filters;

  this.getFilterType = function(){
    return 'compositeFilter';
  }

  this.getFilter = function(){
    return this;
  }

  }

function PropertyFilter(name, operator, value){
    this.propertyFilter = {
                            property: { name: name },
                            operator: operator,
                            value: { stringValue: value}
                          };
    this.getFilterType = function(){
      return 'propertyFilter';
  }

    this.getFilter = function(){
    return this.propertyFilter;
  }
}


  /**
   * Authorise with Google Datastore
   *
   * params:
   * config - datastore specific configuration
   * cb - callback
   */
  function authorise(config,cb) {
    if( !config.DATASTORE_SERVICE_ACCOUNT ||
        !config.DATASTORE_PRIVATE_KEY_FILE ||
        !config.SERVICE_ACCOUNT_SCOPE ||
        !config.DATASETID) {
      var err = new Error("Incomplete Datastore Authentication details");
      seneca.log.error(err);
      return cb(err);
    }

    var authCredentials = new googleapis.auth.JWT(config.DATASTORE_SERVICE_ACCOUNT,
                                          config.DATASTORE_PRIVATE_KEY_FILE,
                                          null,
                                          config.SERVICE_ACCOUNT_SCOPE,
                                          null);

    authCredentials.authorize(function(jwtErr) {
      if (jwtErr) {
          seneca.log.error(jwtErr);
          cb(jwtErr);
        }
        connect(config.DATASETID, authCredentials, cb);
      });
  };



  /**
   * Connect to the Google Datastore and retrieve datastore object

   * params:
   * datasetid - dataset identification code
   * authCredentials - Authentication details for datastore
   * cb - callback
   *
   */
  function connect(datasetid, authCredentials, cb){
  //TODO: Maybe pass in discover parameters at run time.
  //Depends on if the Google API becomes stable 
    googleapis.discover('datastore', 'v1beta2')
        .withAuthClient(authCredentials)
        .execute(function(err, client) {
          if (err) {
            seneca.log.error(err);
            return cb(err);
          }
          datastore = client.datastore.withDefaultParams({
            datasetId: datasetid}).datasets;
          cb(null);
        });
}


/**
 * Start a new transaction.
 * params
 * options - Transaction options
 * cb - callback
 */
function beginTransaction(options, cb){
  datastore.beginTransaction(options)
  .execute(function(err, result) {
    if (err) {
      seneca.log.error(err);
      return;
    }
    cb(result.transaction);
  });
}

/**
 * Create a collection name
 *
 * The underlying database needs to have a name for the
 * table or collection (A 'kind' in Google Datastore) associated
 * with the database. The convention is to join the base and
 * name with an underscore, '_'
 * params
 * entity - The Seneca entity
 */
function createCollectionName(entity){
  var canon = entity.canon$({object:true});
  var kind = (canon.base ? canon.base + '_' : '') + canon.name;
  return kind;
}


/**
 * Properly format the seneca entity attributes for insertion into Google Datastore
 * as properties
 *
 * params
 * entity - The Seneca entity
 */
function createProperties(entity){
  var properties = {};
  var fields = entity.fields$();
  fields.forEach(function(field){
  // TODO: Improve the value type recognition in this method.
  // Only inputting stringValues at the moment
    properties[field] = {stringValue: entity[field]};
  });
  return properties;
}


/**
 * Create the request body for the save operation
 *
 * params
 * entity - The Seneca entity
 * transactionID - The transaction identifier
 */
function createSaveRequest(entity, transactionID){
  var kind = createCollectionName(entity);
  var properties = createProperties(entity);
  var saved = isSaved(entity);
  var path = saved ? [{ kind: kind, id: entity.id}] : [{ kind: kind}];
  var datastoreEntity = {
                          key: { path: path },
                          properties: properties
                        }

  var mutationType = saved ? 'update' : 'insertAutoId';

  var mutation = {};
  mutation[mutationType] = [datastoreEntity];

  var request = {
                  transaction: transactionID,
                  mutation: mutation
                }

  return request;
}


/**
 * Create a query for a database operation
 *
 * params
 * kind - The Google Datastore entity type
 * query - Object containing the properties to search on
 */
function createQuery(kind, query){
  var filters = [];
  for(var p in query){
    if(query.hasOwnProperty(p)){
      var pf = new PropertyFilter(p, 'EQUAL', query[p]);
      filters.push(pf);
    }
  }

  var q = new Query([{'name': kind}], 1);
  if(filters.length == 1){
    q.addFilter(filters[0]);
  }else{
    var cf = new CompositeFilter('AND', filters);
    q.addFilter(cf);
  }

  return q;

}


/**
 * Check whether a seneca entity has been saved to the database
 *
 * params
 * entity - The Seneca entity
 *
 */
function isSaved(entity){
  // If id exists the entity has been saved before.
  return entity.hasOwnProperty('id');
}

  /**
   * the simple db store interface returned to seneca
   */
  var store = {
    name: name,

  /**
     * close the connection
     *
     * params
     * cb - callback
     */
    close: function(cb) {
      datastore = null;
      cb(null);
    },

  /**
     * save the data as specified in the ent attribute of the args object
     * params
     * args - 
     * cb - callback
     */
    save: function(args,cb) {
      var senecaEntity = args.ent;

      beginTransaction({}, function(transactionID){
        var request = createSaveRequest(senecaEntity, transactionID);
        datastore.commit(request)
        .execute(function(err, result){
          if(err){
            seneca.log.error('error', {'Datastore save error': err});
            // TODO: Add in error investigation and rollback functionality
            // datastore.rollback({transaction: transactionID});
            cb(err);
          }else{
            if(isSaved(senecaEntity)){
              seneca.log.debug('updated', senecaEntity);
            }else{
              senecaEntity['id'] = result.mutationResult.insertAutoIdKeys[0].path[0].id;
              seneca.log.debug('saved', senecaEntity);
              cb(null, senecaEntity);
            }

          }
        });
      });

  },

  /**
     * load first matching item based on matching property values
     * in the q attribute of the args object
     * params
     * args - 
     * cb - callback
     */
    load: function(args,cb) {
      var senecaEntity = args.ent;
      var kind = createCollectionName(senecaEntity);
      var query = createQuery(kind, args.q);

      datastore.runQuery(query)
      .execute(function(err, result) {
        if (err) {
          seneca.log.error('error', {'Datastore load query error': err});
          return cb(err);
        }
        var entityResults = result.batch.entityResults || [];
        if (entityResults.length){
          // Only loading one result
          var entity = entityResults[0];
          seneca.log.debug('loaded', entity);
          cb(null, entity);
        }
        else{
          seneca.log('Load: No result');
          cb(err);
        }
    });
  },

   /**
     * return a list of object based on the supplied query, if no query is supplied
     * then all items are selected
     *
     * params
     * args - of the form { ent: { id: , ..entity data..} }
     * cb - callback
     */
    list: function(args,cb) {
      console.log("list function invoked");
      cb(null);
    },

  /**
     * delete an item
     *
     * params
     * args -
     * cb - callback
     */
    remove: function(args,cb) {
        var senecaEntity = args.ent;
        var kind = createCollectionName(senecaEntity);
        var query = createQuery(kind, args.q);

        // Cannot run a query in a transaction so get a reference
        // to the database entity first
        datastore.runQuery(query)
        .execute(function(err, result) {
          if (err) {
            seneca.log.error('error', {'Datastore remove query error': err});
            return cb(err);
          }

          var entityResults = result.batch.entityResults || [];
          if (entityResults.length){
            // Only loading one result
            var id = entityResults[0].entity.key.path[0].id;
            // When we have a reference to the entity id
            // delete the entity inside a transaction
            beginTransaction({}, function(transactionID){
                var path = {path: [{kind: kind, id: id}]};
                var request = {
                        transaction: transactionID,
                        mutation: { delete: [path] }
                      }

                datastore.commit(request)
                .execute(function(err, result){
                  if(err){
                    seneca.log.error('error', {'Datastore remove error': err});
                    // TODO: Add in error investigation and rollback functionality
                    // datastore.rollback({transaction: transactionID});
                    cb(err);
                  }else{
                    seneca.log.debug('removed', path);
                    cb(null);
                  }
                });
              });
          }
          else{
            seneca.log('Remove: No result');
            cb(err);
          }
      })
    },

   /**
     * return the underlying native connection object
     *
     * params
     * args - of the form { ent: { id: , ..entity data..} }
     * cb - callback
     */
    native: function(args,cb) {
      console.log("native function invoked");
      cb(null);
    }
  }


 /**
   * initialization
   */
  var meta = seneca.store.init(seneca,options,store);
  desc = meta.desc;

  seneca.add({init:store.name,tag:meta.tag},function(args,done){
    authorise(options,function(err){
      if( err ) {
        return seneca.fail({code:'entity/authorise',store:store.name,error:err,desc:desc},done);
      }
      else{
        done();
      }
    })
  });


  return {name:store.name,tag:meta.tag};
}
