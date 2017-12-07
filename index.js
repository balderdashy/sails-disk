/**
 * Module dependencies
 */

var _ = require('@sailshq/lodash');
var async = require('async');
var nedb = require('nedb');
var path = require('path');
var Filesystem = require('machinepack-fs');

var normalizeWhere = require('./lib/normalize-where');

/**
 * @scottmac/sails-disk
 *
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
      schema: false,
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
        dbs: {},
        // We'll keep track of any auto-increment sequences in this dictionary, indexed by table name.
        sequences: {},
        // We'll keep track of the primary keys of each model in this datastore in this dictionary,
        // indexed by table name.
        primaryKeyCols: {},
        // We'll keep track of every `ref` column in each model in this datastore in this dictionary,
        // indexed by table name.
        refCols: {}
      };

      // Add the datastore to the `datastores` dictionary.
      datastores[identity] = datastore;

      (function determineDiskOrMemory (proceed) {

        if (datastoreConfig.inMemoryOnly === true) {
          return proceed();
        }
        // Ensure that the given folder exists
        Filesystem.ensureDir({ path: datastoreConfig.dir }).exec(function(err) {
          if (err) {return proceed(err);}
          return proceed();
        });

      }) (function (err) {
        if (err) { return cb(err); }

        // Create a new NeDB instance for each model (an NeDB instance is like one MongoDB collection),
        // and load the instance from disk.  The `loadDatabase` NeDB method is asynchronous, hence the async.each.
        async.each(_.keys(models), function(modelIdentity, next) {

          // Get the model definition.
          var modelDef = models[modelIdentity];

          var primaryKeyAttr = modelDef.definition[modelDef.primaryKey];

          // Ensure that the model's primary key has either `autoIncrement` or `required`
          if (primaryKeyAttr.required !== true && (!primaryKeyAttr.autoMigrations || primaryKeyAttr.autoMigrations.autoIncrement !== true)) {
            return next(new Error('In model `' + modelIdentity + '`, primary key `' + modelDef.primaryKey + '` must have either `required` or `autoIncrement` set.'));
          }

          // Get the model's primary key column.
          var primaryKeyCol = modelDef.definition[modelDef.primaryKey].columnName;

          // Store the primary key column in the datastore's primary key columns hash.
          datastore.primaryKeyCols[modelDef.tableName] = primaryKeyCol;

          // Declare a var to hold the table's sequence name (if any).
          var sequenceName = null;

          // Create the nedb instance and save it to the `modelDbs` hash
          var nedbConfig;
          if (datastoreConfig.inMemoryOnly) {
            nedbConfig = { inMemoryOnly: true };
          } else {
            nedbConfig = { filename: path.resolve(datastoreConfig.dir, modelDef.tableName + '.db') };
          }
          var db = new nedb(nedbConfig);

          datastore.dbs[modelDef.tableName] = db;

          try {
            // Add any unique indexes and initialize any sequences.
            _.each(modelDef.definition, function(val, attributeName) {

              // If the attribute has `unique` set on it, or it's the primary key, add a unique index.
              if ((val.autoMigrations && val.autoMigrations.unique) || (attributeName === modelDef.primaryKey)) {
                if (val.autoMigrations && val.autoMigrations.unique && (!val.required && (attributeName !== modelDef.primaryKey))) {
                  throw new Error('\nIn attribute `' + attributeName + '` of model `' + modelIdentity + '`:\n' +
                                  'When using sails-disk, any attribute with `unique: true` must also have `required: true`\n');
                }
                db.ensureIndex({
                  fieldName: val.columnName,
                  unique: true
                });
              }
              // Otherwise, remove any index that may have been added previously.
              else {
                db.removeIndex(val.columnName);
              }

              // If the attribute has `autoIncrement` on it, and it's the primary key,
              // initialize a sequence for it.
              if (val.autoMigrations && val.autoMigrations.autoIncrement && (attributeName === modelDef.primaryKey)) {
                sequenceName = modelDef.tableName + '_' + val.columnName + '_seq';
                datastore.sequences[sequenceName] = 0;
              }

              datastore.refCols[modelDef.tableName] = datastore.refCols[modelDef.tableName] || [];
              // If the attribute is a ref, save it to the `refCols` dictionary.
              if (val.type === 'ref') {
                datastore.refCols[modelDef.tableName].push(val.columnName);
              }

            });//</ _.each() >

          } catch (e) { return next(e); }

          // Load the database from disk.  NeDB will replay any add/remove index calls before loading the data,
          // so making `loadDatabase` the last step ensures that we can safely migrate data without violating
          // key constraints that have been removed.
          db.loadDatabase(function(err) {
            if (err) { return next(err); }
            // If there's a sequence for this table, then load the records in reverse PK order
            // to get the last sequence value.
            if (sequenceName) {
              var sortObj = {};
              sortObj[primaryKeyCol] = -1;
              // Find the record with the highest PK value.
              db.find({}).sort(sortObj).limit(1).exec(function(err, records) {
                if (err) { return next(err); }
                // No records yet?  Leave the sequence at zero.
                if (records.length === 0) { return next(err); }
                // Otherwise set the sequence to the PK value.
                datastore.sequences[sequenceName] = records[0][primaryKeyCol];
                return next();
              });//_∏_
              return;
            }//-•

            return next();
          });

        }, cb);//</ async.each() >

      });//</ self-calling function>
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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // If there is a sequence for this table, and the column name referenced in the table
      // does not have a value set, set it to the next value of the sequence.
      var primaryKeyCol = datastore.primaryKeyCols[query.using];
      var sequenceName = query.using + '_' + primaryKeyCol + '_seq';
      if (!_.isUndefined(datastore.sequences[sequenceName]) && _.isNull(query.newRecord[primaryKeyCol]) || query.newRecord[primaryKeyCol] === 0) {
        query.newRecord[primaryKeyCol] = ++datastore.sequences[sequenceName];
      }

      // If the primary key col for this table isn't `_id`, set `_id` to the primary key value.
      if (primaryKeyCol !== '_id') { query.newRecord._id = query.newRecord[primaryKeyCol]; }

      // Insert the documents into the db.
      db.insert(query.newRecord, function(err, newRecord) {
        if (err) {
          if (err.errorType === 'uniqueViolated') {
            err.footprint = {
              identity: 'notUnique'
            };
            // If we can infer which attribute this refers to, add a `keys` array to the error.
            // First, see if only one value in the new record matches the value that triggered the uniqueness violation.
            if (_.filter(_.values(query.newRecord), function (val) {return val === err.key;}).length === 1) {
              // If so, find the key (i.e. column name) that this value was assigned to, add set that in the `keys` array.
              err.footprint.keys = [_.findKey(query.newRecord, function(val) {return val === err.key;})];
            } else {
              err.footprint.keys = [];
            }
          }
          return cb(err);
        }
        if (query.meta && query.meta.fetch) {
          // If the primary key col for this table isn't `_id`, exclude it from the returned records.
          if (primaryKeyCol !== '_id') { delete newRecord._id; }
          return cb(undefined, newRecord);
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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // Get the primary key column for thie table.
      var primaryKeyCol = datastore.primaryKeyCols[query.using];

      // Get the possible sequence name for this table.
      var sequenceName = query.using + '_' + primaryKeyCol + '_seq';

      var newRecords = _.map(query.newRecords, function(newRecord) {

        // If there is a sequence and `null` is being sent in for this record's primary key,
        // set it to the next value of the sequence.
        if (!_.isUndefined(datastore.sequences[sequenceName]) && _.isNull(newRecord[primaryKeyCol]) || newRecord[primaryKeyCol] === 0) {
          newRecord[primaryKeyCol] = ++datastore.sequences[sequenceName];
        }

        // If the primary key col for this table isn't `_id`, set `_id` to the primary key value.
        if (primaryKeyCol !== '_id') { newRecord._id = newRecord[primaryKeyCol]; }

        return newRecord;
      });

      // Insert the documents into the db.
      db.insert(newRecords, function(err, newRecords) {
        if (err) {
          if (err.errorType === 'uniqueViolated') {
            err.footprint = {
              identity: 'notUnique',
              keys: []
            };
          }
          return cb(err);
        }
        if (query.meta && query.meta.fetch) {
          // If the primary key col for this table isn't `_id`, exclude it from the returned records.
          if (primaryKeyCol !== '_id') {
            newRecords = _.map(newRecords, function(newRecord) {delete newRecord._id; return newRecord;});
          }
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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      var primaryKeyCol = datastore.primaryKeyCols[query.using];

      // Normalize the stage-3 query criteria into NeDB (really, MongoDB) criteria.
      var where = normalizeWhere(query.criteria.where);

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

      // If the primary key col for this table isn't `_id`, exclude it from the returned records.
      if (primaryKeyCol !== '_id') {
        projection._id = 0;
      }

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
        // Does this model contain any attributes with type `ref`?
        if (datastore.refCols[query.using].length > 0) {
          // If so, loop through the records and transform refs to Buffers where possible.
          _.each(records, function(record) {
            _.each(datastore.refCols[query.using], function(colName) {
              // If this looks like NeDB's idea of a serialized Buffer, turn it into a real buffer.
              if (record[colName] && record[colName].type === 'Buffer' && _.isArray(record[colName].data)) {
                record[colName] = new Buffer(record[colName].data);
              }
            });
          });
        }
        // If the primary key column is `_id`, and we had a projection with just `_id`, transform the records
        // to only contain that column.  This is a workaround for an issue in NeDB where doing a projection
        // with just _id returns all the columns.
        if (primaryKeyCol === '_id' && _.keys(projection).length === 1 && projection._id === 1) {
          records = _.map(records, function(record) {return _.pick(record, '_id');});
        }
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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // Get the primary key column for thie table.
      var primaryKeyCol = datastore.primaryKeyCols[query.using];

      // Normalize the stage-3 query criteria into NeDB (really, MongoDB) criteria.
      var where = normalizeWhere(query.criteria.where);

      // If the user is attempting to change the primary key, do a drop/add instead.
      if (query.valuesToSet[primaryKeyCol]) {
        // Don't allow updating _id, since nedb doesn't support it.
        if (primaryKeyCol === '_id') { return cb(new Error('Cannot change primary key using sails-disk adapter when the primary key column is `_id`.')); }
        // Find the record in question.
        adapter.find(datastoreName, _.cloneDeep(query), function(err, records) {
          if (err) { return cb(err); }
          // Shortcut for when there is no matching record.
          if (records.length === 0) { return cb(undefined, (query.meta && query.meta.fetch) ? [] : undefined); }
          // If more than one record matches, throw an error since you can't update multiple records to have the same PK value.
          if (records.length > 1) { return cb(new Error('Cannot use `.update()` to change the primary key when the query matches multiple records.')); }
          // Get the single returned record.
          var record = records[0];
          // Destroy the record.
          adapter.destroy(datastoreName, _.cloneDeep(query), function(err) {
            if (err) { return cb(err); }
            // Remove the _id field; `.create()` will set it for us.
            delete record._id;
            // Update the record values with those that were sent in with the original `update` query.
            _.extend(record, query.valuesToSet);
            // Create a new record with the updated values.
            adapter.create(datastoreName, { using: query.using, newRecord: record, meta: query.meta }, function(err, record) {
              if (err) {return cb(err);}
              return cb(undefined, record ? [record] : undefined);
            });
          });
        });
        return;
      }

      // If the primary key col for this table isn't `_id`, set `_id` to the primary key value.
      if (primaryKeyCol !== '_id' && query.valuesToSet[primaryKeyCol]) { query.valuesToSet._id = query.valuesToSet[primaryKeyCol]; }

      // Update the documents in the db.
      db.update(where, {'$set': query.valuesToSet}, {multi: true, returnUpdatedDocs: true}, function(err, numAffected, updatedRecords) {
        if (err) {
          if (err.errorType === 'uniqueViolated') {
            err.footprint = {
              identity: 'notUnique'
            };
            // If we can infer which attribute this refers to, add a `keys` array to the error.
            // First, see if only one value in the updated data matches the value that triggered the uniqueness violation.
            if (_.filter(_.values(query.valuesToSet), function (val) {return val === err.key;}).length === 1) {
              // If so, find the key (i.e. column name) that this value was assigned to, add set that in the `keys` array.
              err.footprint.keys = [_.findKey(query.valuesToSet, function(val) {return val === err.key;})];
            } else {
              err.footprint.keys = [];
            }
          }
          return cb(err);
        }//-•
        if (query.meta && query.meta.fetch) {
          // If the primary key col for this table isn't `_id`, exclude it from the returned records.
          if (primaryKeyCol !== '_id') {
            updatedRecords = _.map(updatedRecords, function(updatedRecord) {delete updatedRecord._id; return updatedRecord;});
          }

          return cb(undefined, updatedRecords);
        }//-•
        return cb();
      });

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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // If `fetch` is true, find the records BEFORE we remove them so that we can
      // send them back to the caller.
      (function maybeFetchRecords(done) {

        if (query.meta && query.meta.fetch) {
          adapter.find(datastoreName, _.cloneDeep(query), function(err, records) {
            if (err) { return cb(err); }
            return done(records);
          });
        }
        else {
          return done();
        }
      })(function afterMaybeFetchingRecords(records) {

        // ~∞%°
        // Now, destroy the records.

        // Normalize the stage-3 query criteria into NeDB (really, MongoDB) criteria.
        var where = normalizeWhere(query.criteria.where);

        // Remove the documents from the db.
        db.remove(where, {multi: true}, function(err /*, numAffected */) {
          if (err) { return cb(err); }

          // If `fetch` was true, `records` will hold the records we just destroyed.
          // (otherwise, it will be `undefined`)
          return cb(undefined, records);

        });
      });//</ self-invoking function w/ callback >

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

      adapter.find(datastoreName, query, function(err, records) {

        if (err) { return cb(err); }

        var sum = _.reduce(records, function(memo, row) { return memo + row[query.numericAttrName]; }, 0);
        var avg = sum / records.length;
        return cb(undefined, avg);

      });

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

      adapter.find(datastoreName, query, function(err, records) {

        if (err) { return cb(err); }

        var sum = _.reduce(records, function(memo, row) { return memo + row[query.numericAttrName]; }, 0);
        return cb(undefined, sum);

      });


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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // Get the nedb for the table in question.
      var db = datastore.dbs[query.using];

      // Normalize the stage-3 query criteria into NeDB (really, MongoDB) criteria.
      var where = normalizeWhere(query.criteria.where);

      // Count the documents into the db.
      db.count(where, cb);

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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      var db;

      // If memory only, create a new in-memory nedb for the collection.
      if (datastore.config.inMemoryOnly === true) {
        db = new nedb({ inMemoryOnly: true });
      } else {
        db = new nedb({ filename: path.resolve(datastore.config.dir, tableName + '.db') });
      }

      datastore.dbs[tableName] = db;

      // Re-create any unique indexes.
      _.each(definition, function(val, columnName) {
        // If the attribute has `unique` set on it, or it's the primary key, add a unique index.
        if (val.unique || val.primaryKey) {
          db.ensureIndex({
            fieldName: columnName,
            unique: true
          });
        }
      });

      return db.loadDatabase(cb);

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
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // If memory only, just remove the reference to the nedb for the collection.
      if (datastore.config.inMemoryOnly === true) {
        delete datastore.dbs[tableName];
        return cb();
      }

      // Delete the datastore file.
      var filename = path.resolve(datastore.config.dir, tableName + '.db');
      Filesystem.rmrf({ path: filename }).exec(function(err) {
        if (err) {return cb(err);}
        delete datastore.dbs[tableName];
        return cb();
      });

    },

    //  ╔═╗╔═╗╔╦╗  ┌─┐┌─┐┌─┐ ┬ ┬┌─┐┌┐┌┌─┐┌─┐
    //  ╚═╗║╣  ║   └─┐├┤ │─┼┐│ │├┤ ││││  ├┤
    //  ╚═╝╚═╝ ╩   └─┘└─┘└─┘└└─┘└─┘┘└┘└─┘└─┘
    setSequence: function setSequence(datastoreName, sequenceName, sequenceValue, cb) {

      // Get a reference to the datastore.
      var datastore = datastores[datastoreName];
      if (!datastore) { return cb(new Error('Unrecognized datastore: `'+datastoreName+'`,  It doesn\'t seem to have been registered with this adapter (sails-disk).')); }

      // Set the sequence.
      datastore.sequences[sequenceName] = sequenceValue;

      return cb();

    }
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

