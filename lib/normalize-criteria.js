/**
 * Module dependencies
 */

var _ = require('@sailshq/lodash');

module.exports = function normalizeCriteria (criteria) {

  return _.reduce(criteria.where ? criteria.where : criteria, function(memo, val, key) {

    switch (key) {

      case '<':
        memo['$lt'] = val;
        break;

      case '<=':
        memo['$lte'] = val;
        break;

      case '>':
        memo['$gt'] = val;
        break;

      case '>=':
        memo['$gte'] = val;
        break;

      case '!=':
        memo['$ne'] = val;
        break;

      case 'nin':
        memo['$nin'] = val;
        break;

      case 'in':
        memo['$in'] = val;
        break;

      case 'like':
        memo['$regex'] = _.escapeRegExp(val).replace(/^%/, '.*').replace(/([^\\])%/g, '$1.*').replace(/\\%/g, '%');
        break;

      case 'and':
        memo['$and'] = _.map(val, normalizeCriteria);
        break;

      case 'or':
        memo['$or'] = _.map(val, normalizeCriteria);
        break;

    }

    return memo;

  }, {});

};
