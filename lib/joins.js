/**
 * Module dependencies
 */

var util = require('util');
var _ = require('lodash');
var async = require('async');
var _defaultsDeep = require('merge-defaults');



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
      parentResults: parentResults,
      $find: $find,
      $getPK: $getPK
    }, next);
  }, function _afterwards(err) {
    if (err) return cb(err);

    // Parent records are modified in-place, so we can just send them back.
    return cb(null, parentResults);
  });





  ///// <private methods> ////////////////////////////////////////////

  // TODO:
  // replace the following private methods with a cleaned-up version
  // that can be used in any adapter.


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

  ///// </private methods> ////////////////////////////////////////////
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
                        .$find()  {Function}
                        .$getPK() {Function}

 * @param  {Function} cb
 *
 */
function _joinOneParticularAssoc (options, cb) {

  // Create local variables from the options to make
  // the code below more human-readable
  var attrName = options.attrName;
  var instructions = options.instructions;
  var parentResults = options.parentResults;
  var $find = options.$find;
  var $getPK = options.$getPK;
  

  // If no join instructions were provided, we're done!
  if (instructions.length === 0) {
    return cb(null, parentResults);
  }

  console.log(
    'Preparing to populate the "%s" attr for %d parent result(s)...',
    attrName, parentResults.length
  );


  // ------------------------- (((•))) ------------------------- //

  //
  // Step 1:
  // Plan the query.
  // 

  // Lookup relevant collection identities and primary keys
  var parentIdentity = _.first(instructions).parent;
  var childIdentity = _.last(instructions).child;
  var parentPK = $getPK(parentIdentity);
  var childPK = $getPK(childIdentity);

  // For convenience, precalculate the array of primary key values
  // from the parent results for use in the association strategy
  // implementation code below.
  var parentResultPKVals = _.pluck(parentResults, parentPK);

  // Lookup the base child criteria
  // (populate..where, populate..limit, etc.)
  //
  // Note that default limit, etc. should not be applied here
  // since they are taken care of in Waterline core.
  var childCriteria = _.last(instructions).criteria || {};

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

  if (!strategy) {
    return cb(new Error('Could not derive association strategy in adapter'));
  }

  // Now lookup strategy-specific association metadata.

  // `parentFK` will only be meaningful if this is the `HAS_FK` strategy.
  var parentFK = instructions[0].parentKey;

  // `childFK` will only be meaningful if this is the `VIA_FK` strategy.
  var childFK = instructions[0].childKey;

  // `junctorIdentity`, `junctorFKToParent`, `junctorFKToChild`, and `junctorPK`
  // will only be meaningful if this is the `VIA_JUNCTOR` strategy.
  var junctorIdentity = instructions[0].child;
  var junctorPK = $getPK(instructions[0].child);
  var junctorFKToParent = instructions[0].childKey;
  var junctorFKToChild = instructions[1] && instructions[1].parentKey;


  // IMPORTANT:
  // If the child criteria has a `sort`, `limit`, or `skip`, then we must execute
  // N child queries; where N is the number of parent results.
  // Otherwise the result set will not be accurate.
  var canCombineChildQueries = !!(
    childCriteria.sort  ||
    childCriteria.limit ||
    childCriteria.skip
  );

  // SKIP THIS STEP ENTIRELY FOR NOW
  // TODO: implement this optimization
  canCombineChildQueries = false;



  // ------------------------- (((•))) ------------------------- //


  // Step 2:
  // Build up a set of buffer objects, each representing a find (or for the VIA_JUNCTOR
  // strategy, a nested find), and where the results from that find should be injected-
  // i.e. the related parent record(s) and the name of the attribute.
  var buffers = _.reduce(parentResults,
    function _buildBuffersUsingParentResults (buffers, parentRecord) {
      buffers.push({
        attrName: attrName,
        belongsToPKValue: parentRecord[parentPK],

        // Optional (only used if implementing a HAS_FK strategy)
        belongsToFKValue: parentRecord[parentFK]
      });
      return buffers;
    },
  []);


  // ------------------------- (((•))) ------------------------- //




  //
  // Step 3:
  // Communicate with the datastore to grab relevant child records.
  //

  (function fetchRelevantChildRecords(_onwards) {

    if (canCombineChildQueries) {

      // Special case for VIA_JUNCTOR:
      if (strategy === VIA_JUNCTOR) {
        return next(new Error('via_junctor not implemented yet'));
      }
      else {
        switch (strategy) {
          case HAS_FK:
            _where[childPK] = _.pluck(parentResults, parentFK);
            return _where;
          case VIA_FK:
            _where[childFK] = _.pluck(parentResults, parentPK);
            return _where;
        }
      }
      return _onwards(new Error('not implemented yet!'));
    }


    // Now execute the queries
    async.each(buffers, function (buffer, next){

      // •••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••
      // NOTE:
      // This step could be optimized by calculating the query function
      // ahead of time since we already know the association strategy it
      // will use before runtime.
      // •••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••

      // Special case for VIA_JUNCTOR:
      if (strategy === VIA_JUNCTOR) {
        return next(new Error('via_junctor not implemented yet'));
      }
      // General case for the other strategies:
      else {
        
        var criteriaToPopulateBuffer =
        _defaultsDeep((function _buildBufferCriteriaChangeset () {
          return {
            where: (function _buildBufferWHERE (_where){
              switch (strategy) {
                case HAS_FK:
                  _where[childPK] = buffer.belongsToFKValue;
                  return _where;
                case VIA_FK:
                  _where[childFK] = buffer.belongsToPKValue;
                  return _where;
              }
            })({})
          };
        })(), childCriteria);

        console.log(
          'Populating buffer for parent record "%s" using the following criteria: \n',
          buffer.belongsToPKValue,
          util.inspect(criteriaToPopulateBuffer, false, null)
        );

        $find( childIdentity, criteriaToPopulateBuffer,
        function _afterFetchingBufferRecords(err, childRecordsForThisBuffer) {
          if (err) return next(err);
          
          buffer.records = childRecordsForThisBuffer;
          return next();
        });
      }

    }, _onwards);

  })(function _afterwards(err) {
    if (err) return cb(err);



    // ------------------------- (((•))) ------------------------- //

    // Step 4:
    // Smash each child buffer into the appropriate spot
    // within the parent results.
    // 
    // NOTE: parent results are modified in-place

    if (canCombineChildQueries) {
      // switch (strategy) {
      //   case HAS_FK:
      //     // TODO
      //     break;


      //   case VIA_FK:
      //     // TODO
      //     break;


      //   case VIA_JUNCTOR:
      //     // TODO
      //     break;
      // }
      return cb(new Error('not implemented yet!'), parentResultsBeingAugmented);
    }

    console.log('\n\n\n--------BUFFERS--------\n',util.inspect(buffers, false, null));
    // return cb(new Error('see logs'));

    _.each(buffers, function (buffer){
      if (buffer.records && buffer.records.length) {
        
        var matchingParentRecord = _.find(parentResults, function (parentRecord) {
          return parentRecord[parentPK] === buffer.belongsToPKValue;
        });

        // This should always be true, but checking just in case.
        if (_.isObject(matchingParentRecord)) {

          // If the value in `attrName` for this record is not an array,
          // it is probably a foreign key value.  Fortunately, at this point
          // we can go ahead and replace it safely since any logic relying on it
          // is complete (i.e. although we may still have other queries finishing
          // up for other association attributes, we're done populating THIS one, see?)
          //
          // In fact, and for the same reason, we can safely override the value of
          // `buffer.attrName` for the parent record at this point, no matter what!
          // This is nice, because `buffer.records` is already sorted, limited, and
          // skipped, so we don't have to mess with that.
          matchingParentRecord[buffer.attrName] = buffer.records;
        }
      }
    });

    // Done!
    // (parent records are modified in place, no need to pass anything back.)
    return cb();
  });





}






  // switch (strategy) {


  //   case HAS_FK:

  //     var parentFK = '???TODO???';

  //     $find(childIdentity, _.merge(_.cloneDeep(childCriteria), {
        
  //       where: (function _build_HAS_FK_where_clause (){
  //         var _where = {};
  //         //
  //         // TODO: FIX THIS-- see "IMPORTANT" note above about populate..sort, etc.
  //         // 
  //         _where[childPK] = parentResultPKVals;
  //         return _where;
  //       })()

  //     }), function (err, childResults){
  //       if (err) return cb(err);

  //       // Now integrate.
  //       // 
  //       // We'll join parent results whose relevant foreign key value
  //       // points to any of these child results.
  //       // (since in a hasFK association, the parent result holds the foreign key)
  //       var childPKValues = _.pluck(childResults, childPK);

  //       // For each parent result, inject an appropriately-named field key
  //       // with an array containing the related subset of these child results.
  //       var augmentedParentResults = _.map(parentResults, function (parentResult) {

  //         // TODO: get related subset of child results, e.g. a modified version of:
  //         // -----// Now that we have the linked parent results from this batch, we can
  //         // -----// use them to look up ALL of the possible child results:
  //         // -----childResults = _.where(childResults, function (subResult) {
  //         // -----  return _.contains(_.pluck(relatedParentRecords, foreignKey), subResult[otherRelation.primaryKey]);
  //         // -----});
  //         var relatedChildResults = [];
          
  //         parentResult[options.attrName] = relatedChildResults;
  //       });

  //       // All done.
  //       return cb(null, augmentedParentResults);
        
  //     });



  //     break;








  //   case VIA_FK:

  //     var childFK = '???TODO???';

  //     $find(childIdentity, {}, function (err, childResults){
  //       if (err) return cb(err);
        
  //       // TODO: implement all this
  //       var augmentedParentResults;

  //       // All done.
  //       return cb(null, augmentedParentResults);
  //     });


  //     break;






  //   case VIA_JUNCTOR:

  //     var junctorIdentity = '???TODO???';
  //     var junctorPK = $getPK(junctorIdentity);

  //     var junctorFKToParent = '???TODO???';
  //     var junctorFKToChild  = '???TODO???';

  //     $find(junctorIdentity, {}, function (err, junctorResults){
  //       if (err) return cb(err);

  //       // Finally, fetch related child results matching the populate.
  //       $find(childIdentity, {}, function (err, childResults){
  //         if (err) return cb(err);

  //         // TODO: implement all this
  //         var augmentedParentResults;
          
  //         // All done.
  //         return cb(null, augmentedParentResults);
  //       });
  //     });
  //     break;
    
  // }



// Example of strategy case statement for use elsewhere:
// 
// switch (strategy) {
//   case HAS_FK:
//     break;
//   case VIA_FK:
//     break;
//   case VIA_JUNCTOR:
//     break;
// }






  // var baseBufferCriteria = _.merge(_.cloneDeep(childCriteria), {
  //   where: (function _build_HAS_FK_where_clause (){
  //     var _where = {};
  //     switch (strategy) {
  //       case HAS_FK:
  //         _where[childPK] = parentResultPKVals;
  //         break;
  //       case VIA_FK:
  //         _where[childFK] = parentResultPKVals;
  //         break;
  //       case VIA_JUNCTOR:
  //         // TODO
  //         break;
  //     }
  //     return _where;
  //   })()
  // });


  // // Build array of desired child record buffers
  // var buffers =

  // canCombineBuffers ? buffers.concat([{
  //   address: {
  //     attrName: options.attrName,
  //     // The fact that there is no `pkValue` in this buffer
  //     // will be used as a signal later on to use a more
  //     // intelligent integration strategy.
  //   },
  //   criteria: (function _buildBufferCriteria (){
  //     return _.merge({}, baseBufferCriteria, {
  //       where: {}
  //     });
  //   })(),
  //   records: []
  // }])



  // ---------------------------------------------------------------------------

  // One way to do it:
  //
  // Inject an intermediate step that uses another case statement to build up
  // a set of more detailed instruction objects, each representing a find (or for
  // the VIA_JUNCTOR strategy, a nested find) and where the results from that find
  // should be injected for each parent result.
  // 
  // Then replace [Step 2] as it currently stands with a simple runner that builds
  // up a buffer to hold the result set for each of the new instructions.
  // 
  // Finally, we'd create a new [Step 4], which is a simplified version of the
  // integrator.  Its job is to dump the contents of the buffer(s) into the appropriate
  // parent result(s).
  // ---------------------------------------------------------------------------
