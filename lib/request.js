/* Copyright (c) 2010-2014 John Roche, MIT License */
'use strict';
var convert = require('./convert');
var typeofis = require('type-of-is');

/**
 * Create the request body for the save operation
 *
 * params
 * entity - The Seneca entity
 * kind - The collection name
 * transactionID - The transaction identifier
 */
exports.createSaveRequest = function(entity, kind, transactionID){
  var properties = createProperties(entity),
      path = createKey(entity, kind),
      datastoreEntity = {
                          key: { path: path },
                          properties: properties
                        },
      mutation = {},
      request = {},
      mutationType = entity.id ? 'update' : getInsertType(entity.id$);


  entity.id$ && delete entity.id$;
  mutation[mutationType] = [datastoreEntity];
  request.transaction = transactionID;
  request.mutation = mutation;

  return request;
};


/**
 * Create the request body for the remove operation
 *
 * params
 * transactionID - The transaction identifier
 * keys - list of entity keys to delete
 */
exports.createRemoveRequest = function(transactionID, keys){
  return {
            transaction: transactionID,
            mutation: { delete: keys }
          };
};


/**
 * Create the key for the entity
 *
 * params
 * entity - The Seneca entity
 * kind - The collection name
 */
 function createKey(entity, kind){
  var id = entity.id$ || entity.id,
      re = /[^0-9]/;
      
  if(!id){
    return [{ kind: kind}];
  // Check for non-numeric ids
  }else if(typeofis.string(id) === 'String' && re.test(id)){
    return [{ kind: kind, name:id}];
  }else{
    return [{ kind: kind, id:id}];
  }
  
}

/**
 * Properly format the seneca entity attributes for insertion into Google Datastore
 * as properties
 *
 * params
 * entity - The Seneca entity
 */
function createProperties(entity){
  var copy = entity.clone$(entity),
      properties = {},
      fields;

  // don't re-save id as a property
  delete copy.id;

  fields = copy.fields$();
  fields.forEach(function(field){
    properties[field] = convert.createValueObject(copy[field]);
  });

  return properties;
}


/**
 * Properly format the seneca entity attributes for insertion into Google Datastore
 * as properties
 *
 * params
 * isInsert - Value indicating whether operation is an insert or insertAutoId
 */
function getInsertType(isInsert){
  return isInsert ? 'insert' : 'insertAutoId';
}