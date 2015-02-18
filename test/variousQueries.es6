var _ = require('lodash');

var randomString = require('./helpers/randomString');
var querySequence = require('../src/querySequence');

var scoresLoadFixture = require('./fixtures/scoresLoad');
var variousQueriesFixture = require('./fixtures/variousQueries');

// Create a nodeunit test for each query case
_.forOwn(variousQueriesFixture.cases, (details, caseId) => {
  exports['variousQueries_' + caseId] = function(test) {
    printDebug && console.log('BEGINNING VARIOUS QUERY', caseId);
    scoresLoadFixture.install(variousQueriesFixture.data, (error, result) => {
      if(error) throw error;
      let select = triggers.select(details.query);
      let updateLog = [], // Cache for any updates to this query
          nextLogPos = 0; // Length at last action performed
      select.on('update', (diff, data) => updateLog.push({ diff, data }));
      // For each event, check values or perform action, then continue
      let processEvents = (callback, index) => {
        index = index || 0;
        // Check if at end of event list
        if(index === details.events.length) return callback();
        let event = details.events[index];
        _.forOwn(event, (data, eventType) => {
          printDebug && console.log('EVENT', eventType);
          switch(eventType){
            case 'data':
            case 'diff':
              if(updateLog.length === nextLogPos){
                // No update yet since action
                setTimeout(() => {
                  processEvents(callback, index);
                }, 100);
              }else{
                // New update has arrived, check against data or diff
                test.deepEqual(updateLog[nextLogPos][eventType], data,
                  `Difference on ${eventType} #${nextLogPos}`);
                // Move to next event
                processEvents(callback, index + 1);
              }
              break
            case 'perform':
              nextLogPos = updateLog.length;
              querySequence(client, data, (error, results) => {
                if(error) throw error;
                // Move to next event
                processEvents(callback, index + 1);
              });
              break
            case 'unchanged':
              setTimeout(() => {
                test.equal(updateLog.length, nextLogPos,
                  `Unexpected update on "unchanged" event #${nextLogPos}: 
                    ${JSON.stringify(updateLog[updateLog.length - 1])}`);
                // Move to next event
                processEvents(callback, index + 1);
              }, data);
              break
            default:
              throw new Error('Invalid event type: ' + eventType)
              break
          }
        })
        
      }
      processEvents(() => test.done())
    })
  }
})

