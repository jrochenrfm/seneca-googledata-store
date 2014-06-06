/* Copyright (c) 2014 John Roche */
"use strict";


// var assert = require('assert');
// var async = require('async');


var seneca = require('seneca');
var shared = require('seneca-store-test');

var si = seneca();

var SCOPES = ['https://www.googleapis.com/auth/userinfo.email',
              'https://www.googleapis.com/auth/datastore'];

var options = {
                'DATASTORE_SERVICE_ACCOUNT': '800084035768-68b220nvdotineslk8rs2qmj6lp9kemh@developer.gserviceaccount.com',
                'DATASTORE_PRIVATE_KEY_FILE': '../gcds.pem',
                'SERVICE_ACCOUNT_SCOPE': SCOPES,
                'DATASETID': 'delta-clarity-593'
               };

si.use(require('..'), options);

si.__testcount = 0
var testcount = 0

describe('googledata', function(){
  this.timeout(20000);
  it('basic', function(done){
    testcount++;
    shared.basictest(si,done);
  })

  // it('extra', function(done){
  //   testcount++
  //   extratest(si,done)
  // })

  it('close', function(done){
    shared.closetest(si,testcount,done);
  })
})



// function extratest(si,done) {
//   console.log('EXTRA')

//   async.series(
//     {
//       native: function(cb){
//         var foo = si.make$('foo')
//         foo.native$(function(err,db){
//           assert.ok(null==err)

//           db.collection('foo',function(err,coll){
//             assert.ok(null==err)

//             coll.find({},{},function(err,cursor){
//               assert.ok(null==err)

//               cursor.each(function (entry) {
//                 if (!entry) {
//                   cb()
//                 }
//               })
//             })
//           })
//         })
//       },

//       native_query: function(cb){
//         var nat = si.make$('nat')
//         nat.remove$({all$:true}, function(err){
//           assert.ok(null==err)

//           nat.a=1
//           nat.save$(function(err,nat){
//             assert.ok(null==err)

//             nat = nat.make$()
//             nat.a=2
//             nat.save$(function(err,nat){
//               assert.ok(null==err)

//               nat.list$({native$:[{/*$or:[{a:1},{a:2}]*/},{sort:[['a',-1]]}]},function(err,list){
//                 assert.ok(null==err)
//                 //console.log(list)
//                 assert.equal(2,list.length)
//                 assert.equal(2,list[0].a)
//                 assert.equal(1,list[1].a)
//                 cb()
//               })
//             })
//           })
//         })
//       },

//       remove: function (cb) {
//         var cl = si.make$('lmt')
//         // clear 'lmt' collection
//         cl.remove$({all$: true}, function (err, foo) {
//           assert.ok(null == err)
//           cb()
//         })
//       },

//       insert1st: function (cb) {
//         var cl = si.make$('lmt')
//         cl.p1 = 'v1'
//         cl.save$(function (err, foo) {
//           assert.ok(null == err)
//           cb()
//         })
//       },

//       insert2nd: function (cb) {
//         var cl = si.make$('lmt')
//         cl.p1 = 'v2'
//         cl.save$(function (err, foo) {
//           assert.ok(null == err)
//           cb()
//         })
//       },

//       insert3rd: function (cb) {
//         var cl = si.make$('lmt')
//         cl.p1 = 'v3'
//         cl.save$(function (err, foo) {
//           assert.ok(null == err)
//           cb()
//         })
//       },

//       listall: function (cb) {
//         var cl = si.make({name$: 'lmt'})
//         cl.list$({}, function (err, lst) {
//           assert.ok(null == err)
//           assert.equal(3, lst.length)
//           cb()
//         })
//       },

//       listlimit1skip1: function (cb) {
//         var cl = si.make({name$: 'lmt'})
//         cl.list$({limit$: 1, skip$: 1}, function (err, lst) {
//           assert.ok(null == err)
//           assert.equal(1, lst.length)
//           cb()
//         })
//       },

//       listlimit2skip3: function (cb) {
//         var cl = si.make({name$: 'lmt'})
//         cl.list$({limit$: 2, skip$: 3}, function (err, lst) {
//           assert.ok(null == err)
//           assert.equal(0, lst.length)
//           cb()
//         })
//       },

//       listlimit5skip2: function (cb) {
//         var cl = si.make({name$: 'lmt'})
//         cl.list$({limit$: 5, skip$: 2}, function (err, lst) {
//           assert.ok(null == err)
//           assert.equal(1, lst.length)
//           cb()
//         })
//       },

//       insertUpdate: function (cb) {

//         var cl = si.make$('lmt')
//         cl.p1 = 'value1'
//         cl.p2 = 2
//         cl.save$(function (err, foo) {
//           assert.ok(null == err)
//           assert.ok(foo.id)
//           assert.equal(foo.p1, 'value1')
//           assert.equal(foo.p2, 2)

//           delete foo.p1
//           foo.p2 = 2.2

//           foo.save$(function (err, foo) {
//             assert.ok(null == err)

//             foo.load$({id: foo.id}, function(err, foo) {

//               assert.ok(foo.id)
//               assert.equal(foo.p1, 'value1')
//               assert.equal(foo.p2, 2.2)
//             })
//             cb()
//           })
//         })
//       }
//     },
//     function (err, out) {
//       si.__testcount++
//       done()
//     }
//   )

//   si.__testcount++
// }