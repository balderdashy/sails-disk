/*---------------------------------------------------------------
  :: DiskAdapter
  -> adapter

  This disk adapter is for development only!
---------------------------------------------------------------*/

var Database = require('./database');

module.exports = (function () {

  // Hold connections for this adapter
  var connections = {};

  var adapter = {

    identity: 'sails-disk',

    // Whether this adapter is syncable (yes)
    syncable: true,

    // How this adapter should be synced
    migrate: 'alter',

    // Allow a schemaless datastore
    defaults: {
      schema: false,
      filePath: '.tmp/'
    },

    // Register A Connection
    registerConnection: function (connection, collections, cb) {

      if(!connection.identity) return cb(new Error('Connection is missing an identity'));
      if(connections[connection.identity]) return cb(new Error('Connection is already registered'));

      connections[connection.identity] = new Database(connection, collections);
      connections[connection.identity].initialize(cb);
    },

    // Teardown a Connection
    teardown: function (conn, cb) {
      if(!connections[conn]) return cb();
      delete connections[conn];
      cb();
    },

    // Return attributes
    describe: function (conn, coll, cb) {
      grabConnection(conn).describe(coll, cb);
    },

    define: function (conn, coll, definition, cb) {
      grabConnection(conn).createCollection(coll, definition, cb);
    },

    drop: function (conn, coll, relations, cb) {
      grabConnection(conn).dropCollection(coll, relations, cb);
    },

    find: function (conn, coll, options, cb) {
      grabConnection(conn).select(coll, options, cb);
    },

    create: function (conn, coll, values, cb) {
      grabConnection(conn).insert(coll, values, cb);
    },

    update: function (conn, coll, options, values, cb) {
      grabConnection(conn).update(coll, options, values, cb);
    },

    destroy: function (conn, coll, options, cb) {
      grabConnection(conn).destroy(coll, options, cb);
    }

  };

  /**
   * Grab the connection object for a connection name
   *
   * @param {String} connectionName
   * @return {Object}
   * @api private
   */

  function grabConnection(connectionName) {
    return connections[connectionName];
  }

  return adapter;
})();
