/* Copyright (c) 2014 John Roche */
"use strict";


var seneca = require('seneca');
var shared = require('seneca-store-test');

var si = seneca();

var SCOPES = ['https://www.googleapis.com/auth/userinfo.email',
              'https://www.googleapis.com/auth/datastore'];

var options = {
                'DATASTORE_SERVICE_ACCOUNT': '',
                'DATASTORE_PRIVATE_KEY_FILE': '',
                'SERVICE_ACCOUNT_SCOPE': SCOPES,
                'DATASETID': ''
               };

si.use(require('..'), options);

si.__testcount = 0
var testcount = 0

describe('googledata', function(){
  this.timeout(35000);

  it('basic', function(done){
    testcount++;
    shared.basictest(si,done);
  })

  it('close', function(done){
    shared.closetest(si,testcount,done);
  })
})