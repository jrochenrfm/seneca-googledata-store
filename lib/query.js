
/* Copyright (c) 2010-2014 John Roche, MIT License */
'use strict';


var convert = require('./convert');
var typeofis = require('type-of-is');

/**
 * Create a query for a Google Datastore operation
 *
 * params
 * kind - The Google Datastore entity type
 * query - Object containing the properties to search on and query filters
 * limit - Number of results to limit to
 */
exports.createQuery = function(kind, query, limit){
  var sort = query.sort$,
      limit = limit || query.limit$,
      offset = query.skip$,
      projections = query.fields$,
      q = new Query([{'name': kind}]),
      property,
      direction,
      filter;


  if(sort){
    property = Object.keys(query.sort$).pop();
    direction = sort[property];
    q.setOrder(new Order(property, direction));
    delete query.sort$;
  }

  if(limit){
    q.setLimit(limit);
    delete query.limit$;
  }

  if(offset){
    q.setOffset(offset);
    delete query.skip$;
  }

  if(projections){
    projections.forEach(function(p){
      q.addProjection(new Projection(p));
    });
    delete query.fields$;
  }

  filter = createFilter(query, kind);
  if(filter){
    q.setFilter(filter);
  }

  return q;

};


/**
 * Create a filter object to search on certain properties 
 *
 * params
 * kind - The Google Datastore entity type
 * query - Object containing the properties to search on
 */
function createFilter(query, kind){
  var filters = [],
      re = /[^0-9]/,
      pf,
      cf,
      key;
      
  for(var p in query){
    if(query.hasOwnProperty(p)){
      // To query on id, use the entity key and the
      // special property '__key__' 
      if(p === 'id'){
        // Check for non-numeric ids
        if(typeofis.string(p) === 'String' && re.test(p)){
          key = {path: [{kind: kind, name: query[p]}]};
        }else{
          key = {path: [{kind: kind, id: query[p]}]};
        }
        pf = new PropertyFilter('__key__', 'EQUAL', key);
      }else{
        pf = new PropertyFilter(p, 'EQUAL', query[p]);
      }
      filters.push(pf);
    }
  }

  if(filters.length === 0){
    return null;
  }
  else if(filters.length == 1){
    return filters.pop();
  }else{
    cf = new CompositeFilter('AND', filters);
    return cf;
  }
}


/**
 * Create a new Query object
 *
 * params
 * kinds - The Google Datastore entity types
 */
function Query(kinds){
  this.query = {
    kinds: kinds,
    order: [],
    projection: []
  };

  this.setFilter = function(filter){
    this.query.filter = {};
    this.query.filter[filter.getFilterType()] = filter.getFilter();
  };

  this.addProjection = function(projection){
    this.query.projection.push(projection);
  };

  this.setOrder = function(order){
    this.query.order.push(order);
  };

  this.setLimit = function(limit){
    this.query.limit = limit;
  };

  this.setOffset = function(offset){
    this.query.offset = offset;
  };
}


/**
 * Create a new CompositeFilter object
 *
 * params
 * operator - The operator for combining multiple filters.
 *            Only "and" is currently supported.
 * filters - The list of filters to combine. Must contain at least one filter.
 */
function CompositeFilter(operator, filters){
    this.operator = operator;
    this.filters = filters;

  this.getFilterType = function(){
    return 'compositeFilter';
  };

  this.getFilter = function(){
    return this;
  };

  }


/**
 * Create a new PropertyFilter object
 *
 * params
 * name - The name of the property to filter by
 * operator - The operator to filter by. One of lessThan, lessThanOrEqual,
 *            greaterThan, greaterThanOrEqual, equal, or hasAncestor
 * value - The value to compare the property to.
 */
function PropertyFilter(name, operator, value){
  if(name == '__key__'){
    this.propertyFilter = {
                            property: { name: name },
                            operator: operator,
                            value: {keyValue: value}
                          };
  }else{
    this.propertyFilter = {
                            property: { name: name },
                            operator: operator,
                            value: convert.createValueObject(value)
                          };
    }


    this.getFilterType = function(){
      return 'propertyFilter';
  };

    this.getFilter = function(){
    return this.propertyFilter;
  };
}


/**
 * Create a new Order object
 *
 * params:
 * property - The database property to order by
 * direction - The direction to order by. 1 for ascending, -1 for descending
 */
function Order(property, direction){
  this.property = { name: property };
  this.direction = (direction === 1) ? 'ascending' : 'descending';
}

/**
 * Create a new Projection object
 * Projection queries allow you to query the Datastore for
 * just those specific properties of an entity that you actually need
 *
 * params:
 * property- The database property to project
 * aggregationFunction (optional) - The aggregation function to apply to the property
 */
function Projection(property, aggregationFunction){
  this.property = { name: property };
  if(aggregationFunction){
    this.aggregationFunction = aggregationFunction;
  }
}