/* Copyright (c) 2014 John Roche */
'use strict';


var seneca = require('seneca');
var shared = require('seneca-store-test');
var assert = require('assert');
var async = require('async');


var si = seneca();

var SCOPES = ['https://www.googleapis.com/auth/userinfo.email',
              'https://www.googleapis.com/auth/datastore'];
/**
* Enter your Google Datastore details
*/
var options = {
                'DATASTORE_SERVICE_ACCOUNT': '',
                'DATASTORE_PRIVATE_KEY_FILE': '',
                'SERVICE_ACCOUNT_SCOPE': SCOPES,
                'DATASETID': ''
               };

si.use(require('..'), options);

si.__testcount = 0;
var testcount = 0;

describe('googledata', function(){
  this.timeout(35000);

  it('basic', function(done){
    testcount++;
    shared.basictest(si,done);
  });

  it('extra', function(done){
    testcount++;
    extraTest(si,done);
  });

  it('close', function(done){
    shared.closetest(si,testcount,done);
  });
});


function extraTest(si, done){
  console.log('Extra');
  assert.notEqual(si, null);

  async.series(
  {
    nullOrUndefined: function(cb){
      console.log('Testing null and undefined values');

      var foo = si.make$('foo');
      foo.nul = null;
      foo.und = undefined;

      foo.save$(function(err, saveOut){
        if(err) return cb(err);

        assert.equal(saveOut.nul, null);
        assert.equal( saveOut.und, undefined);
        
        saveOut.load$(saveOut.id, function(err, loadOut){
          if(err) return cb(err);

          assert.equal(loadOut.nul, null);
          assert.equal(loadOut.und, undefined);
          cb(null);
        });
      });
    },

    nonNumericId: function(cb){
      console.log('Testing non-numeric ids');

      var foo = si.make$('foo');
      foo.id$ = 'f65d2c68-4e91-4bce-849b-439adf2460aa';

      foo.save$(function(err, saveOut){
        if(err) return cb(err);

        assert.equal(saveOut.id, 'f65d2c68-4e91-4bce-849b-439adf2460aa');
        
        saveOut.load$(saveOut.id, function(err, loadOut){
          if(err) return cb(err);

          assert.equal(loadOut.id, 'f65d2c68-4e91-4bce-849b-439adf2460aa');
          cb(null);
        });
      });
    },

    cleanUp: function(cb){
      console.log('cleaning up extra');
      var foo = si.make$('foo');
      foo.remove$({all$: true}, function(err, results){
        if(err) return cb(err);
        cb(null);
      });
    }
  },

  function(err, results) {
    err = err || null;
    if(err) {
      console.dir(err);
    }
    si.__testcount++;
    assert.equal(err, null);
    done && done();
  });
}
