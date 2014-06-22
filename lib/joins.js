/**
 * Module dependencies
 */

var _ = require('lodash');
var async = require('async');




/**
 * Run joins
 * 
 * @param  {[type]}   db            [description]
 * @param  {[type]}   joins         [description]
 * @param  {[type]}   parentResults [description]
 * @param  {Function} cb            [description]
 * @return {[type]}                 [description]
 */

module.exports = function _runJoins (db, joinInstructions, parentResults, cb) {

  // Group the joinInstructions array by "alias", then interate over each one
  // s.t. `instructions` in our lambda function contains a list of join instructions
  // for the particular `populate` on the specified logical attribute (i.e. alias).
  // 
  // Note that `parentResults` will be mutated inline.
  var joinsByAssociation = _.groupBy(joinInstructions, 'alias');
  async.each( _.keys(joinsByAssociation), function eachAssociation( attrName, next ) {

    _joinOneParticularAssoc({
      attrName: attrName,
      instructions: joinsByAssociation[attrName],
      parentResults: parentResults
    }, next);
  }, function _afterwards(err) {
    if (err) return cb(err);
    return cb(null, parentResults);
  });
};



/**
 * Association strategy constants
 */
var HAS_FK = 1;
var VIA_FK = 2;
var VIA_JUNCTOR = 3;


/**
 * _joinOneParticularAssoc()
 * 
 * @param  {Object}   options
                        .attrName
                        .instructions
                        .parentResults

 * @param  {Function} cb
 */
function _joinOneParticularAssoc (options, cb) {
  
  // If no join instructions were provided, we're done!
  if (options.instructions.length) {
    return cb(null, options.parentResults);
  }

  console.log('Preparing to populate the "%s" attr for %d parent result(s)...',
    options.attrName, options.parentResults.length);

  //
  // Step 1:
  // Plan the query.
  // 

  // Lookup relevant collection identities and primary keys
  var parentIdentity = _.first(options.instructions).parent;
  var childIdentity = _.last(options.instructions).child;
  var parentPK = $getPK(parentIdentity);
  var childPK = $getPK(childIdentity);

  // For convenience, precalculate the array of primary key values
  // from the parent results for use in the association strategy
  // implementation code below.
  var parentResultPKVals = _.pluck(options.parentResults, parentPK);

  // Lookup the base child criteria
  // (populate..where, populate..limit, etc.)
  //
  // Note that default limit, etc. should not be applied here
  // since they are taken care of in Waterline core.
  var childCriteria = _.last(options.instructions).criteria || {};

  // Determine the type of association rule (i.e. "strategy") we'll be using.
  // 
  // Note that in future versions of waterline, this logic
  // will be internalized to simplify adapter implementation.
  var strategy = (
    // If there are more than one join instructions, there must be an
    // intermediate (junctor) collection involved
    instructions.length === 2 ? VIA_JUNCTOR :
    // If the parent's PK IS the foreign key (i.e. parentKey) specified
    // in the join instructions, we know to use the `viaFK` AR (i.e. belongsToMany)
    instructions[0].parentKey === parentPK ? VIA_FK :
    // Otherwise this is a basic foreign key component relationship
    HAS_FK
  );

  //
  // IMPORTANT:
  // If the child criteria has:
  // 
  // (1) a `sort`, or
  // (2) a `where` AND either a `limit` or `skip` modifier, or both..
  // 
  // We must execute N child queries; where N is the number of parent
  // results. Otherwise the result set will not be accurate.
  // 
  // TODO: handle this, and try to reuse as much of the code below as possible.


  //
  // Step 2:
  // Communicate with the datastore to grab relevant records,
  // then smash them all together.
  //
  switch (strategy) {


    case HAS_FK:

      var parentFK = '???TODO???';

      $find(childIdentity, _.merge(_.cloneDeep(childCriteria), {
        
        where: (function _build_HAS_FK_where_clause (){
          var _where = {};
          _where[childPK] = parentResultPKVals;
          return _where;
        })()

      }), function (err, childResults){
        if (err) return cb(err);

        // Now integrate.
        // 
        // We'll join parent results whose relevant foreign key value
        // points to any of these child results.
        // (since in a hasFK association, the parent result holds the foreign key)
        
        // For each parent result, inject an appropriately-named field key
        // with an array containing the related subset of these child results.
        
        
        var childPKValues = _.pluck(childResults, childPK);
        var relatedParentRecords = _.where(options.parentResults, function (parentResult) {
          return _.contains(childPKValues, parentResult[parentFK]);
        });

        // Now that we have the linked parent results from this batch, we can
        // use them to look up ALL of the possible child results:
        childResults = _.where(childResults, function (subResult) {
          return _.contains(_.pluck(relatedParentRecords, foreignKey), subResult[otherRelation.primaryKey]);
        });
        
      });



      break;








    case VIA_FK:

      var childFK = '???TODO???';

      $find(childIdentity, {}, function (err, childResults){
        if (err) return cb(err);
        
      });


      break;






    case VIA_JUNCTOR:

      var junctorIdentity; // todo: find this
      var junctorPK = $getPK(junctorIdentity);

      $find(junctorIdentity, {}, function (err, junctorResults){
        if (err) return cb(err);

        // Finally, fetch related child results matching the populate.
        $find(childIdentity, {}, function (err, childResults){
          if (err) return cb(err);

          
        });
      });
      break;



    default:
      return cb(new Error('Unknown association strategy passed to adapter'));
  }





  /**
   * Find some records directly (using only this adapter)
   * from the specified collection.
   * 
   * @param  {String}   collectionIdentity
   * @param  {Object}   criteria
   * @param  {Function} cb
   */
  function $find (collectionIdentity, criteria, cb) {
    return db.select(collectionIdentity, criteria, cb);
  }

  /**
   * Look up the name of the primary key field
   * for the collection with the specified identity.
   * 
   * @param  {String}   collectionIdentity
   * @return {String}
   */
  function $getPK (collectionIdentity) {
    if (!collectionIdentity) return;
    return db.getPKField(collectionIdentity);
  }





}
