/**
 * Module dependencies
 */

var _ = require('@sailshq/lodash');
var async = require('async');
var nedb = require('nedb');
var path = require('path');
var Filesystem = require('machinepack-fs');

var normalizeCriteria = require('./lib/normalize-criteria');

/**
 * @scottmac/sails-disk
 *
 * Most of the methods below are optional.
 *
 * If you don't need / can't get to every method, just implement
 * what you have time for.  The other methods will only fail if
 * you try to call them!
 *
 * For many adapters, this file is all you need.  For very complex adapters, you may need more flexiblity.
 * In any case, it's probably a good idea to start with one file and refactor only if necessary.
 * If you do go that route, it's conventional in Node to create a `./lib` directory for your private submodules
 * and load them at the top of the file with other dependencies.  e.g. var update = `require('./lib/update')`;
 */
module.exports = (function sailsDisk () {

  // Private var to track of all the datastores that use this adapter.  In order for your adapter
  // to support advanced features like transactions and native queries, you'll need
  // to expose this var publicly as well.  See the `registerDatastore` method for more info.
  //
  var datastores = {};

  // The main adapter object.
  var adapter = {

    // The identity of this adapter, to be referenced by datastore configurations in a Sails app.
    identity: 'sails-disk',

    // Waterline Adapter API Version
    adapterApiVersion: 1,

    // Default configuration for connections
    defaults: {
      dir: '.tmp/localDiskDb'
    },

    //  ╔═╗═╗ ╦╔═╗╔═╗╔═╗╔═╗  ┌─┐┬─┐┬┬  ┬┌─┐┌┬┐┌─┐
    //  ║╣ ╔╩╦╝╠═╝║ ║╚═╗║╣   ├─┘├┬┘│└┐┌┘├─┤ │ ├┤
    //  ╚═╝╩ ╚═╩  ╚═╝╚═╝╚═╝  ┴  ┴└─┴ └┘ ┴ ┴ ┴ └─┘
    //  ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌┬┐┌─┐┬─┐┌─┐┌─┐
    //   ││├─┤ │ ├─┤└─┐ │ │ │├┬┘├┤ └─┐
    //  ─┴┘┴ ┴ ┴ ┴ ┴└─┘ ┴ └─┘┴└─└─┘└─┘
    // This allows outside access to the datastores, for use in advanced ORM methods like `.runTransaction()`.
    datastores: datastores,

    //  ╦═╗╔═╗╔═╗╦╔═╗╔╦╗╔═╗╦═╗  ┌┬┐┌─┐┌┬┐┌─┐┌─┐┌┬┐┌─┐┬─┐┌─┐
    //  ╠╦╝║╣ ║ ╦║╚═╗ ║ ║╣ ╠╦╝   ││├─┤ │ ├─┤└─┐ │ │ │├┬┘├┤
    //  ╩╚═╚═╝╚═╝╩╚═╝ ╩ ╚═╝╩╚═  ─┴┘┴ ┴ ┴ ┴ ┴└─┘ ┴ └─┘┴└─└─┘
    /**
     * Register a new datastore with this adapter.  This often involves creating a new connection
     * to the underlying database layer (e.g. MySQL, mongo, or a local file).
     *
     * Waterline calls this method once for every datastore that is configured to use this adapter.
     * This method is optional but strongly recommended.
     *
     * @param  {Dictionary}   datastoreConfig  Dictionary of configuration options for this datastore (e.g. host, port, etc.)
     * @param  {Dictionary}   models           Dictionary of model schemas using this datastore.
     * @param  {Function}     cb               Callback after successfully registering the datastore.
     */

    registerDatastore: function registerDatastore(datastoreConfig, models, cb) {

      // Get the unique identity for this datastore.
      var identity = datastoreConfig.identity;
      if (!identity) {
        return cb(new Error('Invalid datastore config. A datastore should contain a unique identity property.'));
      }

      // Validate that the datastore isn't already initialized
      if (datastores[identity]) {
        throw new Error('Datastore `' + identity + '` is already registered.');
      }

      // Create a new datastore dictionary.
      var datastore = {
        config: datastoreConfig,
        // We'll add each model's nedb instance to this dictionary.
        dbs: {}
      };

      // Add the datastore to the `datastores` dictionary.
      datastores[identity] = datastore;

      // Ensure that the given folder exists
      Filesystem.ensureDir({ path: datastoreConfig.dir }).exec(function(err) {
        if (err) {return cb(err);}

        // Create a new NeDB instance for each model (an NeDB instance is like one MongoDB collection)
        _.each(models, function(modelDef, modelIdentity) {

          // Create the nedb instance and save it to the `modelDbs` hash
          var filename = path.resolve(datastoreConfig.dir, modelDef.tableName + '.db');
          var db = new nedb({ filename: filename, autoload: true });
          datastore.dbs[modelDef.tableName] = db;

          // Add any unique indexes
          _.each(modelDef.definition, function(val, attributeName) {
            if (val.autoMigrations && val.autoMigrations.unique) {
              db.ensureIndex({
                fieldName: val.columnName,
                unique: true
              });
            }
          });

        });

        return cb();

      });

    },


    //  ╔╦╗╔═╗╔═╗╦═╗╔╦╗╔═╗╦ ╦╔╗╔  ┌─┐┌─┐┌┐┌┌┐┌┌─┐┌─┐┌┬┐┬┌─┐┌┐┌
    //   ║ ║╣ ╠═╣╠╦╝ ║║║ ║║║║║║║  │  │ │││││││├┤ │   │ ││ ││││
    //   ╩ ╚═╝╩ ╩╩╚══╩╝╚═╝╚╩╝╝╚╝  └─┘└─┘┘└┘┘└┘└─┘└─┘ ┴ ┴└─┘┘└┘
    /**
     * Fired when a datastore is unregistered, typically when the server
     * is killed. Useful for tearing-down remaining open connections,
     * etc.
     *
     * @param  {String} identity  (optional) The datastore to teardown.  If not provided, all datastores will be torn down.
     * @param  {Function} cb     Callback
     */
    teardown: function (identity, cb) {

      var datastoreIdentities = [];

      // If no specific identity was sent, teardown all the datastores
      if (!identity || identity === null) {
        datastoreIdentities = datastoreIdentities.concat(_.keys(datastores));
      } else {
        datastoreIdentities.push(identity);
      }

      // Teardown each datastore
      _.each(datastoreIdentities, function teardownDatastore(datastoreIdentity) {

        // Remove the modelDbs entries for each table that uses this datastore.
        _.each(datastores[datastoreIdentity].tables, function(tableName) {
          delete modelDbs[tableName];
        });

        // Remove the datastore entry.
        delete datastores[datastoreIdentity];

      });

      return cb();

    },


    //  ██████╗  ██████╗ ██╗
    //  ██╔══██╗██╔═══██╗██║
    //  ██║  ██║██║   ██║██║
    //  ██║  ██║██║▄▄ ██║██║
    //  ██████╔╝╚██████╔╝███████╗
    //  ╚═════╝  ╚══▀▀═╝ ╚══════╝
    //
    // Methods related to manipulating data stored in the database.


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   ├┬┘├┤ │  │ │├┬┘ ││
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ┴└─└─┘└─┘└─┘┴└──┴┘
    /**
     * Add a new row to the table
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    create: function create(datastoreName, query, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // Clear out any `null` _id value.
      if (_.isNull(query.newRecord._id)) {
        delete query.newRecord._id;
      }

      // Insert the documents into the db.
      db.insert(query.newRecord, function(err, newRecords) {
        if (err) {return cb(err);}
        if (query.meta && query.meta.fetch) {
          return cb(undefined, newRecords);
        }
        return cb();
      });
    },


    //  ╔═╗╦═╗╔═╗╔═╗╔╦╗╔═╗  ╔═╗╔═╗╔═╗╦ ╦  ┬─┐┌─┐┌─┐┌─┐┬─┐┌┬┐
    //  ║  ╠╦╝║╣ ╠═╣ ║ ║╣   ║╣ ╠═╣║  ╠═╣  ├┬┘├┤ │  │ │├┬┘ ││
    //  ╚═╝╩╚═╚═╝╩ ╩ ╩ ╚═╝  ╚═╝╩ ╩╚═╝╩ ╩  ┴└─└─┘└─┘└─┘┴└──┴┘
    /**
     * Add multiple new rows to the table
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    createEach: function createEach(datastoreName, query, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // Clear out any `null` _id value.
      var newRecords = _.map(query.newRecords, function(newRecord) {
        if (_.isNull(newRecord._id)) {
          delete newRecord._id;
        }
        return newRecord;
      });

      // Insert the documents into the db.
      db.insert(newRecords, function(err, newRecords) {
        if (err) {return cb(err);}
        if (query.meta && query.meta.fetch) {
          return cb(undefined, newRecords);
        }
        return cb();
      });

    },


    //  ╔═╗╔═╗╦  ╔═╗╔═╗╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╚═╗║╣ ║  ║╣ ║   ║   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩═╝╚═╝╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    /**
     * Select Query Logic
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    find: function find(datastoreName, query, cb) {
      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // Normalize the stage-3 query criteria into NeDB (really, MongoDB) criteria.
      var where = normalizeCriteria(query.criteria.where);

      // Transform the stage-3 query sort array into an NeDB sort dictionary.
      var sort = _.reduce(query.criteria.sort, function(memo, sortObj) {
        var key = _.first(_.keys(sortObj));
        memo[key] = sortObj[key].toLowerCase() === 'asc' ? 1 : -1;
        return memo;
      }, {});

      // Transform the stage-3 query select array into an NeDB projection dictionary.
      var projection = _.reduce(query.criteria.select, function(memo, colName) {
        memo[colName] = 1;
        return memo;
      }, {});

      // Create the initial adapter query.
      var findQuery = db.find(where).sort(sort).projection(projection);

      // Add in limit if necessary.
      if (query.criteria.limit) {
        findQuery.limit(query.criteria.limit);
      }

      // Add in skip if necessary.
      if (query.criteria.skip) {
        findQuery.skip(query.criteria.skip);
      }

      // Find the documents in the db.
      findQuery.exec(function(err, records) {
        if (err) {return cb(err);}
        return cb(undefined, records);
      });

    },


    //  ╦ ╦╔═╗╔╦╗╔═╗╔╦╗╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║ ║╠═╝ ║║╠═╣ ║ ║╣   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╩  ═╩╝╩ ╩ ╩ ╚═╝  └─┘└└─┘└─┘┴└─ ┴
    /**
     * Update one or more models in the table
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    update: function update(datastoreName, query, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // When implementing this method, this is where you'll
      // perform the query and return the result, e.g.:
      //
      // datastore.dbConnection.update(query, function(err, result) {
      //   if (err) {return cb(err);}
      //   return cb(undefined, result);
      // });
      //
      // Note that depending on the value of `query.meta.fetch`,
      // you may be expected to return the array of documents
      // that were updated as the second argument to the callback.

      // But for now, this method is just a no-op.
      return cb();

    },


    //  ╔╦╗╔═╗╔═╗╔╦╗╦═╗╔═╗╦ ╦  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //   ║║║╣ ╚═╗ ║ ╠╦╝║ ║╚╦╝  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ═╩╝╚═╝╚═╝ ╩ ╩╚═╚═╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    /**
     * Delete one or more records in a table
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    destroy: function destroy(datastoreName, query, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // When implementing this method, this is where you'll
      // perform the query and return the result, e.g.:
      //
      // datastore.dbConnection.destroy(query, function(err, result) {
      //   if (err) {return cb(err);}
      //   return cb(undefined, result);
      // });
      //
      // Note that depending on the value of `query.meta.fetch`,
      // you may be expected to return the array of documents
      // that were destroyed as the second argument to the callback.

      // But for now, this method is just a no-op.
      return cb();

    },



    //  ╔═╗╦  ╦╔═╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╠═╣╚╗╔╝║ ╦  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╩ ╩ ╚╝ ╚═╝  └─┘└└─┘└─┘┴└─ ┴
    /**
     * Find out the average of the query.
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    avg: function avg(datastoreName, query, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // When implementing this method, this is where you'll
      // perform the query and return the result, e.g.:
      //
      // datastore.dbConnection.find(query, function(err, result) {
      //   if (err) {return cb(err);}
      //   var sum = _.reduce(result, function(memo, row) { return memo + row[query.numericAttrName]; }, 0);
      //   var avg = sum / result.length;
      //   return cb(undefined, avg);
      // });

      // But for now, this method is just a no-op.
      return cb();

    },


    //  ╔═╗╦ ╦╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ╚═╗║ ║║║║  │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╩ ╩  └─┘└└─┘└─┘┴└─ ┴
    /**
     * Find out the sum of the query.
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    sum: function sum(datastoreName, query, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // When implementing this method, this is where you'll
      // perform the query and return the result, e.g.:
      //
      // datastore.dbConnection.find(query, function(err, result) {
      //   if (err) {return cb(err);}
      //   var sum = _.reduce(result, function(memo, row) { return memo + row[query.numericAttrName]; }, 0);
      //   return cb(undefined, sum);
      // });

      // But for now, this method is just a no-op.
      return cb();

    },


    //  ╔═╗╔═╗╦ ╦╔╗╔╔╦╗  ┌─┐ ┬ ┬┌─┐┬─┐┬ ┬
    //  ║  ║ ║║ ║║║║ ║   │─┼┐│ │├┤ ├┬┘└┬┘
    //  ╚═╝╚═╝╚═╝╝╚╝ ╩   └─┘└└─┘└─┘┴└─ ┴
    /**
     * Return the number of matching records.
     * @param  {String}       datastoreName The name of the datastore to perform the query on.
     * @param  {Dictionary}   query         The stage-3 query to perform.
     * @param  {Function}     cb            Callback
     */
    count: function count(datastoreName, query, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // When implementing this method, this is where you'll
      // perform the query and return the result, e.g.:
      //
      // datastore.dbConnection.count(query, function(err, result) {
      //   if (err) {return cb(err);}
      //   return cb(undefined, result);
      // });

      // But for now, this method is just a no-op.
      return cb();

    },


    //  ██████╗ ██████╗ ██╗
    //  ██╔══██╗██╔══██╗██║
    //  ██║  ██║██║  ██║██║
    //  ██║  ██║██║  ██║██║
    //  ██████╔╝██████╔╝███████╗
    //  ╚═════╝ ╚═════╝ ╚══════╝
    //
    // Methods related to modifying the underlying data structure of the
    // database.

    //  ╔╦╗╔═╗╔═╗╦╔╗╔╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║║╣ ╠╣ ║║║║║╣    │ ├─┤├┴┐│  ├┤
    //  ═╩╝╚═╝╚  ╩╝╚╝╚═╝   ┴ ┴ ┴└─┘┴─┘└─┘
    /**
     * Build a new table in the database.
     *
     * (This is used to allow Sails to do auto-migrations)
     *
     * @param  {String}       datastoreName The name of the datastore containing the table to create.
     * @param  {String}       tableName     The name of the table to create.
     * @param  {Dictionary}   definition    The table definition.
     * @param  {Function}     cb            Callback
     */
    define: function define(datastoreName, tableName, definition, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // Create the datastore file.
      var filename = path.resolve(datastore.config.dir, tableName + '.db');
      var db = new nedb({ filename: filename, autoload: true });
      datastore.dbs[tableName] = db;
      return cb();

    },


    //  ╔╦╗╦═╗╔═╗╔═╗  ┌┬┐┌─┐┌┐ ┬  ┌─┐
    //   ║║╠╦╝║ ║╠═╝   │ ├─┤├┴┐│  ├┤
    //  ═╩╝╩╚═╚═╝╩     ┴ ┴ ┴└─┘┴─┘└─┘
    /**
     * Remove a table from the database.
     *
     * @param  {String}       datastoreName The name of the datastore containing the table to create.
     * @param  {String}       tableName     The name of the table to create.
     * @param  {undefined}    relations     Currently unused
     * @param  {Function}     cb            Callback
     */
    drop: function drop(datastoreName, tableName, relations, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];

      // Delete the datastore file.
      var filename = path.resolve(datastore.config.dir, tableName + '.db');
      Filesystem.rmrf({ path: filename }).exec(function(err) {
        if (err) {return cb(err);}
        delete datastore.dbs[tableName];
        return cb();
      });

    },

  };


  if (process.env.DEBUG_QUERY) {
    _.each(adapter, function(val, key) {
      if (_.isFunction(val) && val.toString().match(/^function\s\w+?\(datastoreName, query/)) {
        adapter[key] = function(_null, query) {
          console.log(key.toUpperCase(),'QUERY:');
          console.dir(query, {depth: null});
          console.log('--------\n');
          val.apply(adapter, arguments);
        };
      }
    });
  }

  // Expose adapter definition
  return adapter;

})();

