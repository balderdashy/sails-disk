/**
 * Module dependencies
 */

var _ = require('@sailshq/lodash');

module.exports = function normalizeWhereClause (whereClause) {

  return (function transformBranch(branch) {

    var loneKey = _.first(_.keys(branch));
    var val = branch[loneKey];

    if (loneKey === 'and' || loneKey === 'or') {
      branch['$'+loneKey] = _.map(val, transformBranch);
      delete branch[loneKey];
      return branch;
    }

    if (!_.isObject(val)) {
      return branch;
    }

    var modifier = _.first(_.keys(val));
    var modified = val[modifier];
    delete val[modifier];

    switch (modifier) {

      case '<':
        val['$lt'] = modified;
        break;

      case '<=':
        val['$lte'] = modified;
        break;

      case '>':
        val['$gt'] = modified;
        break;

      case '>=':
        val['$gte'] = modified;
        break;

      case '!=':
        val['$ne'] = modified;
        break;

      case 'nin':
        val['$nin'] = modified;
        break;

      case 'in':
        val['$in'] = modified;
        break;

      case 'like':
        val['$regex'] = new RegExp('^'+_.escapeRegExp(modified).replace(/^%/, '.*').replace(/([^\\])%/g, '$1.*').replace(/\\%/g, '%')+'$');
        break;

    }

    return branch;

  })(whereClause);

};
