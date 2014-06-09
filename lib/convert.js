/* Copyright (c) 2010-2014 John Roche, MIT License */
'use strict';

var typeis = require('type-of-is');

var typeMap = {
	              'Integer': 'integerValue',
	              'Double': 'doubleValue',
	              'Boolean': 'booleanValue',
	              'String': 'stringValue',
	              'Object': 'blobKeyValue',
	              'Array': 'blobKeyValue',
	              'Date': 'dateTimeValue'
	            };

/**
 * Create the property value object for the Google Datastore
 *
 * params
 * value - The value to store in the Datastore
 */
exports.createValueObject = function(value){
  var svt = getStoreValueType(value),
      valueObject = {};

  // TODO: update Array to listValue
 if(svt === 'blobKeyValue'){
     value = JSON.stringify(value);
    }
    
    valueObject[svt] = value;
    
    return valueObject; 
};

/**
 * Convert a Javascript variable type to a corresponding Google Datastore type
 *
 * params
 * v - A variable
 */
function getStoreValueType(v){
    var type = typeis.string(v);
    if(type === 'Number'){
      type = isInteger(v) ? 'Integer' : 'Double';
      }
      return typeMap[type];
    }

/**
 * Check whether a Number is an Integer
 *
 * params
 * n - A Number
 */
function isInteger(n) {
    return n === +n && n === (n|0);
}


/**
 * Translate a database entity into a seneca entity
 *
 * params
 * senecaEntity - A seneca entity to translate into database entity.
 * dbEntity - A database entity
 */
exports.translate = function(senecaEntity, dbEntity){
  var fields = {};

  fields.id = dbEntity.entity.key.path[0].id;
  var properties = dbEntity.entity.properties;
  for(var p in properties){
    if(properties.hasOwnProperty(p)){
      fields[p] = createField(properties[p]);
    }
  }

  return senecaEntity.make$(fields);
};

/**
 * Convert a Google Datastore value object into a Javascript variable
 *
 * params
 * valueOb - A Google Datastore value object
 */
function createField(valueOb){
  var dbType = '',
  	  keys = Object.keys(valueOb);

  for(var i in keys){
    if(keys[i].indexOf('Value') !== -1){
      dbType = keys[i];
    }
  }

  switch(dbType) {
	case 'stringValue':
	    return valueOb.stringValue;
	case 'integerValue':
	    return parseInt(valueOb.integerValue);
	case 'doubleValue':
	    return parseFloat(valueOb.doubleValue);
	case 'booleanValue':
	    return 'true' === dbType;
	case 'dateTimeValue':
	    return new Date(Date.parse(valueOb.dateTimeValue));
	case 'blobKeyValue':
	    return JSON.parse(valueOb.blobKeyValue);    
	default:
	    return;
	  }
}