/**
 * Run Integration Tests
 *
 * Uses the waterline-adapter-tests module to
 * run mocha tests against the currently implemented
 * waterline API.
 */

var tests = require('waterline-adapter-tests'),
    adapter = require('../index'),
    mocha = require('mocha');

/**
 * Build a Config File
 */

var config = {};

/**
 * Expose Interfaces Used In Adapter
 */

var interfaces = ['semantic', 'queryable', 'migratable'];

/**
 * Run Tests
 */

var suite = new tests({ adapter: adapter, config: config, interfaces: interfaces });
