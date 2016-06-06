/**
 * Module dependencies
 */

var _ = require('lodash');
var fsx = require('fs-extra');
var util = require('util');
var async = require('async');
var waterlineCriteria = require('waterline-criteria');
var Aggregate = require('./aggregates');
var Errors = require('waterline-errors').adapter;


/**
 * A File-Backed Datastore
 *
 * @param {Object} config
 * @param {Object} collections
 * @return {Object}
 * @api public
 */

var Database = module.exports = function(config, collections) {
  var self = this;

  // Hold Config values for each collection, this allows each collection
  // to define which file the data is synced to
  this.config = config || {};

  // Build a filePath
  this.filePath = this.config.filePath + (this.config.fileName || (this.config.identity + '.db'));

  // Hold Collections
  this.collections = collections || {};

  // Build an object to hold the data
  this.data = {};

  // Build a Counters Object for Auto-Increment
  this.counters = {};

  // Hold Schema Objects to describe the structure of an object
  this.schema = {};

  return this;
};

/**
 * Initialize Database
 *
 */

Database.prototype.initialize = function(cb) {
  var self = this;

  async.auto({

    checkData: function(next) {
      if(Object.keys(self.data).length > 0) return next();
      self.read(next);
    },

    setCollection: ['checkData', function(next) {
      async.eachSeries(Object.keys(self.collections), function(key, nextCollection) {
        var collection = self.collections[key];
        self.registerCollection(key, collection, nextCollection);
      }, next);
    }]

  }, cb);

};

/**
 * Register Collection
 *
 * @param {String} collectionName
 * @param {Object} collection
 * @param {Function} callback
 * @api public
 */

Database.prototype.registerCollection = function(collectionName, collection, cb) {
  this.setCollection(collectionName, collection, cb);
};

/**
 * Set Collection
 *
 * @param {String} collectionName
 * @param {Object} definition
 * @return Object
 * @api private
 */

Database.prototype.setCollection = function(collectionName, options, cb) {

  // If no filePath is set for this collection, return an error in the object
  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));

  // Set Defaults
  var data = this.data[collectionName] || [];
  data.concat(options.data || []);

  // Ensure data is set
  this.data[collectionName] = data || [];

  // Set counters
  var counters = this.counters[collectionName] = this.counters[collectionName] || {};

  if(options.definition) options.definition = _.cloneDeep(options.definition);
  var schema = this.schema[collectionName] = options.definition || {};

  var obj = {
    data: data,
    schema: schema,
    counters: counters
  };

  cb(null, obj);
};

/**
 * Get Collection
 *
 * @param {String} collectionName
 * @return {Object}
 * @api private
 */

Database.prototype.getCollection = function(collectionName, cb) {

  // If no filePath is set for this collection, return an error in the object
  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));

  var obj = {
    data: this.data[collectionName] || {},
    schema: this.schema[collectionName] || {},
    counters: this.counters[collectionName] || {}
  };

  cb(null, obj);
};

/**
 * Write Data To Disk
 *
 * @param {String} collectionName
 * @param {Function} callback
 * @api private
 */

Database.prototype.write = function(collectionName, cb) {
  var self = this;

  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));

  var data = this.data;
  var schema = this.schema;
  var counters = this.counters;

  fsx.createFileSync(this.filePath);
  fsx.outputJsonSync(self.filePath, { data: data, schema: schema, counters: counters });

  cb();
};

/**
 * Read Data From Disk
 *
 * @param {Function} callback
 * @api private
 */

Database.prototype.read = function(cb) {
  var self = this;

  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));

  var exists = fsx.existsSync(this.filePath);

  if(!exists) {
    fsx.createFileSync(this.filePath);
    return cb(null, { data: {}, schema: {}, counters: {} });
  }

  // Check if we have already read the data file into memory
  if(Object.keys(self.data).length !== 0) return cb(null, {
    data: self.data,
    schema: self.schema,
    counters: self.counters
  });

  var data = fsx.readFileSync(this.filePath, { encoding: 'utf8' });

  if(!data) return cb(null, { data: {}, schema: {}, counters: {} });

  var state;

  try {
    state = JSON.parse(data);
  }
  catch (e) {
    return cb(e);
  }

  // Cast values when reading off of disk.  Essentially, unserialize the records.
  for (var collectionName in state.data) {
    state.data[collectionName].map(function (values) {
      if (self.collections[collectionName]) {
        self.collections[collectionName]._cast.run(values);
      }
    });
  }

  self.data = state.data;
  self.schema = state.schema;
  self.counters = state.counters;

  cb(null, { data: state.data, schema: state.schema, counters: state.counters });
};

///////////////////////////////////////////////////////////////////////////////////////////
/// DDL
///////////////////////////////////////////////////////////////////////////////////////////

/**
 * Register a new Collection
 *
 * @param {String} collectionName
 * @param {Object} definition
 * @param {Function} callback
 * @return Object
 * @api public
 */

Database.prototype.createCollection = function(collectionName, definition, cb) {
  var self = this;

  this.setCollection(collectionName, { definition: definition }, function(err, collection) {
    if(err) return cb(err);

    self.write(collectionName, function() {
      cb(null, collection.schema);
    });
  });
};

/**
 * Describe a collection
 *
 * @param {String} collectionName
 * @param {Function} callback
 * @api public
 */

Database.prototype.describe = function(collectionName, cb) {

  this.getCollection(collectionName, function(err, data) {
    if(err) return cb(err);

    var schema = Object.keys(data.schema).length > 0 ? data.schema : null;
    cb(null, schema);
  });
};

/**
 * Drop a Collection
 *
 * @param {String} collectionName
 * @api public
 */

Database.prototype.dropCollection = function(collectionName, relations, cb) {
  var self = this;

  if(typeof relations === 'function') {
    cb = relations;
    relations = [];
  }

  // If no filePath is set for this collection, return an error in the object
  if(!this.filePath) return {
    error: new Error('No filePath was configured for this collection')
  };

  delete this.data[collectionName];
  delete this.schema[collectionName];

  relations.forEach(function(relation) {
    delete self.data[relation];
    delete self.schema[relation];
  });

  this.write(collectionName, cb);
};

///////////////////////////////////////////////////////////////////////////////////////////
/// DQL
///////////////////////////////////////////////////////////////////////////////////////////

/**
 * Select
 *
 * @param {String} collectionName
 * @param {Object} options
 * @param {Function} cb
 * @api public
 */

Database.prototype.select = function(collectionName, options, cb) {

  // If no filePath is set for this collection, return an error in the object
  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));

  // Filter Data based on Options criteria
  var collectionSchema = this.schema[collectionName] || {};
  var resultSet = waterlineCriteria(collectionName, this.data, options, collectionSchema);

  // console.log('---resultSet---',resultSet);

  // Process Aggregate Options
  var aggregate = new Aggregate(options, resultSet.results);

  if(aggregate.error) return cb(aggregate.error);
  cb(null, aggregate.results);
};

/**
 * Insert A Record
 *
 * @param {String} collectionName
 * @param {Object} values
 * @param {Function} callback
 * @return {Object}
 * @api public
 */

Database.prototype.insert = function(collectionName, values, cb) {

  // If no filePath is set for this collection, return an error in the object
  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));

  var self = this;

  var originalValues = _.clone(values);
  if(!Array.isArray(values)) values = [values];

  // To hold any uniqueness constraint violations we encounter:
  var constraintViolations = [];

  // Iterate over each record being inserted, deal w/ auto-incrementing
  // and checking the uniquness constraints.
  for (var i in values) {
    var record = values[i];

    // Check Uniqueness Constraints
    // (stop at the first failure)
    constraintViolations = constraintViolations.concat(self.enforceUniqueness(collectionName, record));
    if (constraintViolations.length) break;

    // Auto-Increment any values that need it
    record = self.autoIncrement(collectionName, record);
    record = self.serializeValues(collectionName, record);

    if (!self.data[collectionName]) return cb(Errors.CollectionNotRegistered);
    self.data[collectionName].push(record);
  }

  // If uniqueness constraints were violated, send back a validation error.
  if (constraintViolations.length) {
    return cb(new UniquenessError(constraintViolations));
  }

  this.write(collectionName, function() {
    cb(null, Array.isArray(originalValues) ? values : values[0]);
  });
};

/**
 * Update A Record
 *
 * @param {String} collectionName
 * @param {Object} options
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */

Database.prototype.update = function(collectionName, options, values, cb) {
  var self = this;

  // If no filePath is set for this collection, return an error in the object
  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));
  // Filter Data based on Options criteria
  var collectionSchema = this.schema[collectionName] || {};
  var resultSet = waterlineCriteria(collectionName, this.data, options, collectionSchema);

  // Get a list of any attributes we're updating that:
  // i) are in the schema
  // ii) have uniqueness constraints
  // var uniqueAttrs = _.where(_.keys(values), function(attrName) {
  //   var attributeDef = self.schema[collectionName][attrName];
  //   return attributeDef && attributeDef.unique;
  // });

  // If we're updating any attributes that are supposed to be unique, do some additional checks
  // if (uniqueAttrs.length && resultSet.indices.length) {

  //   // If we would be updating more than one record, then uniqueness constraint automatically fails
  //   if (resultSet.indices.length > 1) {
  //     var error = new Error('Uniqueness check failed on attributes: ' + uniqueAttrs.join(','));
  //     return cb(error);
  //   }

  //   // Otherwise for each unique attribute, ensure that the matching result already has the value
  //   // we're updating it to, so that there wouldn't be more than one record with the same value.
  //   else {
  //     var result = self.data[collectionName][resultSet.indices[0]];
  //     var records = _.reject(self.data[collectionName], result);

  //     // Build an array of uniqueness errors
  //     var uniquenessErrors = [];
  //     uniquenessErrors.code = 'E_UNIQUE';

  //     _.each(uniqueAttrs, function eachAttribute (uniqueAttrName) {

  //       // look for matching values in all records. note: this is case sensitive.
  //       if (!!~_.pluck(records, uniqueAttrName).indexOf(values[uniqueAttrName])) {

  //         uniquenessErrors.push({
  //           attribute: uniqueAttrName,
  //           value: values[uniqueAttrName]
  //         });
  //         // errors.push(new Error('Uniqueness check failed on attribute: ' + uniqueAttrName + ' with value: ' + values[uniqueAttrName]));
  //       }
  //     });

  //     // Finally, send the uniqueness errors back to Waterline.
  //     if (uniquenessErrors.length) {
  //       return cb(uniquenessErrors);
  //     }
  //   }
  // }


  // Enforce uniquness constraints
  // If uniqueness constraints were violated, send back a validation error.
  var pkAttrName = getPrimaryKey(this.schema[collectionName]);
  // console.log('pkAttrName',pkAttrName);
  var pkValueOfFirstRecordInResultSet = (function (){
    // console.log('resultSet',resultSet.results);
    if (resultSet.results && resultSet.results[0] && resultSet.results[0][pkAttrName]) {
      return resultSet.results[0][pkAttrName];
    }
    return undefined;
  })();
  var violations = self.enforceUniqueness(collectionName, values, pkValueOfFirstRecordInResultSet);
  if (violations.length) {
    return cb(new UniquenessError(violations));
  }

  // Otherwise, success!
  // Build up final set of results.
  var results = [];
  for (var i in resultSet.indices) {
    var matchIndex = resultSet.indices[i];
    var _values = self.data[collectionName][matchIndex];

    // Clone the data to avoid providing raw access to the underlying
    // in-memory data, lest a user makes inadvertent changes in her app.
    self.data[collectionName][matchIndex] = _.extend(_values, values);
    results.push(_.cloneDeep(self.data[collectionName][matchIndex]));
  }

  self.write(collectionName, function() {
    cb(null, results);
  });
};

/**
 * Destroy A Record
 *
 * @param {String} collectionName
 * @param {Object} options
 * @param {Function} callback
 * @api public
 */

Database.prototype.destroy = function(collectionName, options, cb) {
  var self = this;

  // If no filePath is set for this collection, return an error in the object
  if(!this.filePath) return cb(new Error('No filePath was configured for this collection'));

  // Filter Data based on Options criteria
  var collectionSchema = this.schema[collectionName] || {};
  var resultSet = waterlineCriteria(collectionName, this.data, options, collectionSchema);

  this.data[collectionName] = _.reject(this.data[collectionName], function (model, i) {
    return _.includes(resultSet.indices, i);
  });

  this.write(collectionName, function() {
    cb(null, resultSet.results);
  });
};

///////////////////////////////////////////////////////////////////////////////////////////
/// CONSTRAINTS
///////////////////////////////////////////////////////////////////////////////////////////

/**
 * Auto-Increment values based on schema definition
 *
 * @param {String} collectionName
 * @param {Object} values
 * @return {Object}
 * @api private
 */

Database.prototype.autoIncrement = function(collectionName, values) {

  for (var attrName in this.schema[collectionName]) {
    var attrDef = this.schema[collectionName][attrName];

    // Only apply autoIncrement if we're supposed to!
    if(!attrDef.autoIncrement) continue;

    // Set initial counter value to 0 for this attribute if not set
    if(!this.counters[collectionName][attrName]) this.counters[collectionName][attrName] = 0;

    // If peeps are manually setting the AI field, keep the counter ahead.
    // Otherwise, increment the counter and set the field.
    if(values[attrName]) {

        if (values[attrName] > this.counters[collectionName][attrName]) {
            this.counters[collectionName][attrName] = values[attrName];
        }

    } else {

        // Increment AI counter
        this.counters[collectionName][attrName]++;

        // Set data to current auto-increment value
        values[attrName] = this.counters[collectionName][attrName];
    }

  }

  return values;
};

/**
 * Serialize Values
 *
 * Serializes/Casts values before inserting.
 *
 * @param {Object} values
 * @return {Object}
 * @api private
 */

Database.prototype.serializeValues = function(collectionName, values) {
  var self = this;

  Object.keys(values).forEach(function(key) {

    // Check if a type exist in the schema
    if(!self.schema[collectionName]) return;
    if(!self.schema[collectionName].hasOwnProperty(key)) return;

    var type = self.schema[collectionName][key].type,
        val;

    if(type === 'json') {
      try {
        val = JSON.parse(values[key]);
      } catch(e) {
        return;
      }
      values[key] = val;
    }
  });

  return values;
};





/**
 * enforceUniqueness
 *
 * Enforces uniqueness constraint.
 *
 * PERFORMANCE NOTE:
 * This is O(N^2) - could be easily optimized with a logarithmic algorithm,
 * but this is a development-only database adapter, so this is fine for now.
 *
 * @param {String} collectionName
 * @param {Object} values           - attribute values for a single record
 * @param {*} pkValueOfFirstRecordInResultSet       - (optional) the primary key value of the record we're updating
 * @return {Object}
 * @api private
 */

Database.prototype.enforceUniqueness = function(collectionName, values, pkValueOfFirstRecordInResultSet) {

  var errors = [];

  // Get the primary key attribute name, so as not to inadvertently check
  // uniqueness on something that doesn't matter.
  var pkAttrName = getPrimaryKey(this.schema[collectionName]);

  for (var attrName in this.schema[collectionName]) {
    var attrDef = this.schema[collectionName][attrName];

    if(!attrDef.unique) continue;

    // Loop through each record in our database
    for (var index in this.data[collectionName]) {

      // Ignore uniqueness check on undefined values
      // (they shouldn't have been stored anyway)
      if (_.isUndefined(values[attrName])) continue;

      // Ignore uniqueness check on null values
      // null should be ignored in unique constraint following SQL:2011 standard
      // even so some DBMS (eg. Microsoft SQL Server) are not standard compliant
      if (_.isNull(values[attrName])) continue;

      // If `pkValueOfFirstRecordInResultSet` was provided, this must be an update,
      // and we're updating the record with this pk value.
      // e.g. if I'm updating record w/ id=3 to have email='foo@foo.com',
      // but it already has an email='foo@foo.com', then this is not a
      // uniqueness violation.
      //
      // (note that if I'm updating multiple records but providing only one value for
      // a unique attribute, this will inevitably fail anyways-- this is why we only care
      // about the first record in the result set)
      if (pkValueOfFirstRecordInResultSet) {
        var existingRecord = _.find(this.data[collectionName], function (record){
          return record[pkAttrName] === pkValueOfFirstRecordInResultSet;
        });
        if (existingRecord) {
          // It isn't actually a uniquness violation if the record
          // we're checking is the same as the record we're updating/creating
          if (values[attrName] === existingRecord[attrName]){
            continue;
          }
        }
      }

      // Does it look like a 'uniqueness violation'?
      if (values[attrName] === this.data[collectionName][index][attrName]) {

        // It isn't actually a uniquness violation if the record
        // we're checking is the same as the record we're updating/creating
        if (values[pkAttrName] === this.data[collectionName][index][pkAttrName]) {
          continue;
        }

        var uniquenessError = {
          attribute: attrName,
          value: values[attrName],
          rule: 'unique'
        };

        errors.push(uniquenessError);
      }
    }
  }

  return errors;
};


/**
 * @param  {String} collectionIdentity
 * @return {String}
 * @api private
 */
Database.prototype.getPKField = function (collectionIdentity) {
  return getPrimaryKey(this.schema[collectionIdentity]);
};


/**
 * Convenience method to grab the name of the primary key attribute,
 * given the schema.
 *
 * @param  {Object} schema
 * @return {String}
 * @api private)
 */
function getPrimaryKey (schema) {
  var pkAttrName;
  _.each(schema, function (def, attrName) {
    if (def.primaryKey) pkAttrName = attrName;
  });
  return pkAttrName;
}


/**
 * Given an array of errors, create a WLValidationError-compatible
 * error definition.
 *
 * @param {Array} errors
 * @constructor
 * @api private
 */
function UniquenessError ( errors ) {

  // If no uniqueness constraints were violated, return early-
  // there are no uniqueness errors to worry about.
  if ( !errors.length ) return false;

  //
  // But if constraints were violated, we need to build a validation error.
  //

  // First, group errors into an object of single-item arrays of objects:
  // e.g.
  // {
  //   username: [{
  //     attribute: 'username',
  //     value: 'homeboi432'
  //   }]
  // }
  //
  errors = _.groupBy(errors, 'attribute');
  //
  // Then remove the `attribute` key.
  //
  errors = _.mapValues(errors, function (err) {
    delete err[0].attribute;
    return err;
  });

  // Finally, build a validation error:
  var validationError = {
    code: 'E_UNIQUE',
    invalidAttributes: errors
  };

  // and return it:
  return validationError;

}
