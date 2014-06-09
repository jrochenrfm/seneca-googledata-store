/* Copyright (c) 2010-2014 John Roche, MIT License */
'use strict';

//TODO: one var pattern
var googleapis = require('googleapis');
var query = require('./query');
var convert = require('./convert');
var request = require('./request');


var name = 'googledata-store';


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
* report error conditions and call error callback
*/
function doError(err, cb) {
  seneca.log.error('entity',err,{store:name});
  // TODO: Add in error investigation and rollback functionality
  // datastore.rollback({transaction: transactionID});
  return cb(err);
}


/**
* Process results and call done callback
* params:
* result - Google datastore result object
* cb - callback
*/
function doResult(result, cb){
  var entityResults = getResults(result);
  if (entityResults.length){
      entityResults.forEach(function(entity){
      cb(entity, cb);
    });
  }
}

  /**
   * Authorise with Google Datastore
   *
   * params:
   * config - datastore specific configuration
   * cb - callback
   */
  function authorise(config, cb) {
    if( !config.DATASTORE_SERVICE_ACCOUNT ||
        !config.DATASTORE_PRIVATE_KEY_FILE ||
        !config.SERVICE_ACCOUNT_SCOPE ||
        !config.DATASETID) {
      seneca.log.error('Incomplete Datastore Authentication details');
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
  }



  /**
   * Connect to the Google Datastore and retrieve datastore object

   * params:
   * datasetid - dataset identification code
   * authCredentials - Authentication details for datastore
   * cb - callback
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
  var canon = entity.canon$({object:true}),
      kind = (canon.base ? canon.base + '_' : '') + canon.name;

  return kind;
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
 * Get the entity id from the result object
 *
 * params
 * result - Google Datastore result object
 *
 */
function getId(result){
  return result.mutationResult.insertAutoIdKeys[0].path[0].id;
}

/**
 * Get the list of results from the result object
 *
 * params
 * result - Google Datastore result object
 *
 */
function getResults(result){
  return result.batch.entityResults || [];
}

  /**
   * the simple db store interface returned to seneca
   */
  var store = {

  /**
     * close the connection
     *
     * params
     * cb - callback
     */
    close: function(cb) {
      datastore = null;
      // cb(null);
    },

  /**
     * save the data as specified in the ent attribute of the args object
     * params
     * args - 
     * cb - callback
     */
    save: function(args,cb) {
      var senecaEntity = args.ent,
          kind = createCollectionName(senecaEntity),
          previouslySaved = isSaved(senecaEntity),
          appid$ = senecaEntity.id$,
          rqst;


      beginTransaction({}, function(transactionID){
        rqst = request.createSaveRequest(senecaEntity, kind, transactionID);
        datastore.commit(rqst)
        .execute(function(err, result){
          if(err){
            return doError(err, cb);
          }
          
          if(previouslySaved){
              seneca.log.debug('updated', senecaEntity);
            }else if(appid$){
              senecaEntity.id = appid$;
              seneca.log.debug('saved', senecaEntity);
            }
            else{
              senecaEntity.id = getId(result);
              seneca.log.debug('saved', senecaEntity);
            }
            cb(null, senecaEntity);
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
      var senecaEntity = args.ent,
          kind = createCollectionName(senecaEntity),
          q = query.createQuery(kind, args.q, 1),
          hasResult = false;


      datastore.runQuery(q)
      .execute(function(err, result) {
        if(err){
          return doError(err, cb);
        }

        doResult(result, function(entity){
          entity = convert.translate(senecaEntity, entity);
          seneca.log.debug('loaded', entity);
          cb(null, entity);
          hasResult = true;
        });

        if(!hasResult){
          seneca.log.debug('No result loaded');
          cb(null, null);
        }

    });
  },

   /**
     * return a list of object based on the supplied query, if no query is supplied
     * then all items are selected
     *
     * params
     * args - 
     * cb - callback
     */
    list: function(args,cb) {
      var senecaEntity = args.ent,
          kind = createCollectionName(senecaEntity),
          q = query.createQuery(kind, args.q),
          list = [];


        datastore.runQuery(q)
        .execute(function(err, result) {
          if(err){
            return doError(err, cb);
          }

        doResult(result, function(entity){
          list.push(convert.translate(senecaEntity, entity));
        });

        cb(null, list);
      });
    },

  /**
     * delete an item
     *
     * params
     * args -
     * cb - callback
     */
    remove: function(args,cb) {
        var senecaEntity = args.ent,
            kind = createCollectionName(senecaEntity),
            removeAll = args.q.all$,
            q,
            keys = [],
            rqst;


        if(removeAll){
          delete args.q.all$;
          q = query.createQuery(kind, args.q);
        }
        else{
          q = query.createQuery(kind, args.q, 1);
        }

        datastore.runQuery(q)
        .execute(function(err, result) {
          if(err){
            return doError(err, cb);
          }
          
          doResult(result, function(entity){
            keys.push(entity.entity.key);
          });

          if (keys.length){
            beginTransaction({}, function(transactionID){
                rqst = request.createRemoveRequest(transactionID, keys);
                datastore.commit(rqst)
                .execute(function(err, result){
                  debugger;
                  if(err){
                    return doError(err, cb);
                  }

                  seneca.log.debug('removed', keys);
                  cb(null);
                });
              });
          }
          else{
            seneca.log('Remove: No result');
            cb(err);
          }
      });
    },

   /**
     * return the underlying native connection object
     *
     * params
     * args - of the form { ent: { id: , ..entity data..} }
     * cb - callback
     */
    native: function(args,cb) {
      console.log('native function invoked');
      cb(null);
    }
  };


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
    });
  });


  return {name:store.name,tag:meta.tag};
};