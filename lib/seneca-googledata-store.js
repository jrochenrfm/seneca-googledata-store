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
     * save the data as specified in the entitiy block on the arguments object
     * params
     * args - of the form { ent: { id: , ..entity data..} }
     * cb - callback
     */
    save: function(args,cb) {
      beginTransaction({}, function(transactionID){
      var entity = {
               key: { path: [{ kind: 'Seneca', name: new Date().getMilliseconds() }] },
                        properties: {
                          user: { stringValue: 'Barry' },
                          group:   { stringValue: 'Finance' }
                        }
                      }
      
      var request = {
              transaction: transactionID,
              mutation: { insert: [entity] }
            }

      datastore.commit(request)
      .execute(function(err, result){
        if(err){
          seneca.log.error('error', {'Datastore save error': err});
          // TODO: Add in error investigation and rollback functionality
          // datastore.rollback({transaction: transactionID});
          cb(err);
        }else{
          seneca.log.debug('saved', entity);
          cb(null);
        }
      });
    });
  },

  /**
     * load first matching item based on id
     * params
     * args - of the form { ent: { id: , ..entity data..} }
     * cb - callback
     */
    load: function(args,cb) {
      var entityType = args.qent;
      var query    = args.q;
      var canon = entityType.canon$({object:true})
      var collname = (canon.base?canon.base+'_':'')+canon.name


      beginTransaction({}, function(transactionID){
      var request = {
              readOptions: {
                transaction: transactionID
              },
              keys: [{ path: [{ kind: 'Trivia', name: 'hgtg' }] }]
            }

        datastore.lookup(request)
        .execute(function(err, result) {
          if (err) {
            seneca.log.error('error', {'Datastore load error': err});
            cb(err);
          }
          if (result.found) {
            var entity = result.found[0].entity;
            seneca.log.debug('load', entity);
            cb(null);
          }
      })
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
     * args - of the form { ent: { id: , ..entity data..} }
     * cb - callback
     */
    remove: function(args,cb) {
      beginTransaction({}, function(transactionID){
      var key = { path: [{ kind: 'Seneca', name: 'seneca1' }] };
      
      var request = {
              transaction: transactionID,
              mutation: { delete: [key] }
            }

      datastore.commit(request)
      .execute(function(err, result){
        if(err){
          seneca.log.error('error', {'Datastore remove error': err});
          // TODO: Add in error investigation and rollback functionality
          // datastore.rollback({transaction: transactionID});
          cb(err);
        }else{
          seneca.log.debug('remove', key);
          cb(null);
        }
      });
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
