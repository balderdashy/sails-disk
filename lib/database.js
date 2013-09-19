
/**
 * Module dependencies
 */

var _ = require('lodash'),
    fs = require('fs-extra'),
    async = require('async'),
    waterlineCriteria = require('waterline-criteria'),
    Aggregate = require('./aggregates');

/**
 * A File-Backed Datastore
 *
 * @return {Object}
 * @api public
 */

var Database = module.exports = function() {
  var self = this;

  // Hold Config values for each collection, this allows each collection
  // to define which file the data is synced to
  this.config = {};

  // Build an object to hold the data
  this.data = {};

  // Build a Counters Object for Auto-Increment
  this.counters = {};

  // Hold Schema Objects to describe the structure of an object
  this.schema = {};

  // Create a Write Queue to ensure only 1 write to a file is happening at once
  this.writeQueue = async.queue(function(collectionName, cb) {
    self.write(collectionName, cb);
  }, 1);

  return this;
};

/**
 * Register Collection
 *
 * @param {String} collectionName
 * @param {Object} config
 * @param {Function} callback
 * @api public
 */

Database.prototype.registerCollection = function(collectionName, config, cb) {
  var name = collectionName.toLowerCase(),
      filePath = config.filePath;

  // Set Empty Defaults for the filePath
  if(!this.config[name]) this.config[name] = config;
  if(!this.data[filePath]) this.data[filePath] = {};
  if(!this.counters[filePath]) this.counters[filePath] = {};
  if(!this.schema[filePath]) this.schema[filePath] = {};

  this.read(collectionName, cb);
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
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  // If no filePath is set for this collection, return an error in the object
  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  // Set Defaults
  var data = this.data[filePath][name] = options.data || [];
  var counters = this.counters[filePath][name] = options.counters || {};

  if(options.definition) options.definition = _.cloneDeep(options.definition);
  var schema = this.schema[filePath][name] = options.definition || {};

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
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  // If no filePath is set for this collection, return an error in the object
  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  var obj = {
    data: this.data[filePath][name] || {},
    schema: this.schema[filePath][name] || {},
    counters: this.counters[filePath][name] || {}
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
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  var data = this.data[filePath];
  var schema = this.schema[filePath];
  var counters = this.counters[filePath];

  fs.createFile(filePath, function(err) {
    if(err) return cb(err);
    fs.outputJson(filePath, { data: data, schema: schema, counters: counters }, cb);
  });
};

/**
 * Read Data From Disk
 *
 * @param {String} collectionName
 * @param {Function} callback
 * @api private
 */

Database.prototype.read = function(collectionName, cb) {
  var self = this,
      name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  fs.exists(filePath, function(exists) {
    if(!exists) {
      return fs.createFile(filePath, function(err) {
        if(err) return cb(err);
        cb(null, { data: {}, schema: {}, counters: {} });
      });
    }

    // Check if we have already read the data file into memory
    if(self.data[filePath].length !== 0) return cb(null, {
      data: self.data[filePath],
      schema: self.schema[filePath],
      counters: self.schema[filePath]
    });

    fs.readFile(filePath, { encoding: 'utf8' }, function (err, data) {
      if(err) return cb(err);
      if(!data) return cb(null, { data: {}, schema: {}, counters: {} });

      var state;

      try {
        state = JSON.parse(data);
      }
      catch (e) {
        return cb(e);
      }

      self.data[filePath] = state.data;
      self.schema[filePath] = state.schema;
      self.counters[filePath] = state.counters;

      cb(null, { data: state.data, schema: state.schema, counters: state.counters });
    });
  });
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

    self.writeQueue.push(collectionName);
    cb(null, collection.schema);
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
  var self = this,
      name = collectionName.toLowerCase();

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

Database.prototype.dropCollection = function(collectionName, cb) {
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  // If no filePath is set for this collection, return an error in the object
  if(!filePath) return {
    error: new Error('No filePath was configured for this collection')
  };

  delete this.data[filePath][name];
  delete this.schema[filePath][name];
  delete this.counters[filePath][name];

  this.writeQueue.push(collectionName);
  cb();
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
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  // If no filePath is set for this collection, return an error in the object
  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(name, this.data[filePath], options);

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
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  // If no filePath is set for this collection, return an error in the object
  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  // Check Uniqueness Constraints
  var errors = this.uniqueConstraint(collectionName, values);
  if(errors && errors.length > 0) return cb(errors);

  // Auto-Increment any values
  values = this.autoIncrement(collectionName, values);
  this.data[filePath][name].push(values);

  this.writeQueue.push(collectionName);
  cb(null, values);
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
  var self = this,
      name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  // If no filePath is set for this collection, return an error in the object
  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(name, this.data[filePath], options);
  var results = [];

  resultSet.indicies.forEach(function(matchIndex) {
    var _values = self.data[filePath][name][matchIndex];
    self.data[filePath][name][matchIndex] = _.merge(_values, values);
    results.push(_.cloneDeep(self.data[filePath][name][matchIndex]));
  });

  this.writeQueue.push(collectionName);
  cb(null, results);
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
  var self = this,
      name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  // If no filePath is set for this collection, return an error in the object
  if(!filePath) return cb(new Error('No filePath was configured for this collection'));

  // Filter Data based on Options criteria
  var resultSet = waterlineCriteria(name, this.data[filePath], options);

  this.data[filePath][name] = _.reject(this.data[filePath][name], function (model, i) {
    return _.contains(resultSet.indicies, i);
  });

  this.writeQueue.push(collectionName);
  cb();
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
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  for (var attrName in this.schema[filePath][name]) {
    var attrDef = this.schema[filePath][name][attrName];

    // Only apply autoIncrement if value is not specified
    if(!attrDef.autoIncrement) continue;
    if(values[attrName]) continue;

    // Set Initial Counter Value to 0 for this attribute if not set
    if(!this.counters[filePath][name][attrName]) this.counters[filePath][name][attrName] = 0;

    // Increment AI counter
    this.counters[filePath][name][attrName]++;

    // Set data to current auto-increment value
    values[attrName] = this.counters[filePath][name][attrName];
  }

  return values;
};

/**
 * Unique Constraint
 *
 * @param {String} collectionName
 * @param {Object} values
 * @return {Array}
 * @api private
 */

Database.prototype.uniqueConstraint = function(collectionName, values) {
  var name = collectionName.toLowerCase(),
      filePath = this.config[name].filePath;

  var errors = [];

  for (var attrName in this.schema[filePath][name]) {
    var attrDef = this.schema[filePath][name][attrName];

    if(!attrDef.unique) continue;

    for (var index in this.data[filePath][name]) {

      // Ignore uniquness check on undefined values
      if (_.isUndefined(values[attrName])) continue;

      if (values[attrName] === this.data[filePath][collectionName][index][attrName]) {
        var error = new Error('Uniqueness check failed on attribute: ' + attrName +
          ' with value: ' + values[attrName]);

        errors.push(error);
      }
    }
  }

  return errors;
};
