//NEED THE FS MODULE TO READ THE INPUT FILE
const fs = require('fs');

//CSV PARSE MODULE FOR PARSING THE CSV FILE
const csvparser = require('csv-parse')

//MOMENT MODULE TO CALCULATE THE TIME DIFFERENCE & DO TIME RELATED OPERATIONS
var moment = require('moment');

const inputFile = 'Nodes.csv'
//USING THIS OBJECT TO STORE THE ROWS WITH DISCONNECTED STATUS TEMPORARILY FOR EACH NODE EXPLAINED BELOW -> KEY = NODEID, VALUE = THE COMPLETE ROW VARIABLE WHICH CONSISTS OF ALL THE ATTRRIBUTE VALUES FOR EACH NODE

var node = {}
//USING THIS OBJECT TO STORE THE FINAL RESULT -> KEY = NODEID, VALUE = THE DAYS WHERE NODE DID NOT COMMUNICATE WITH THE SERVER (I HAVE MADE IT UNIQUE USING SETS)

var result = {}
//THE PARSING OPERATION STARTS HERE
var parser = csvparser({delimiter: ','}, function (err, data) {
    data.forEach(function(line) {
      var nodeData = { "sequenceId" : line[0]
                    , "nodeId" : line[1]
                    , "date" : line[2]
                    , "status" : line[3]
                    };
//STORING THE NODEID WITH A EMPTY SET WHEN WHEN EACH NEW NODE IS ENCOUNTERED
    if(!(nodeData.nodeId in result)) {
        var noCommunicationDays = new Set();
         result[nodeData.nodeId] = noCommunicationDays;
    }
//CHECKING IF THE CURRENT NODEID ALREADY EXISTS IN THE NODE OBJECT DECLARED EARLIER
    if (!(nodeData.nodeId in node) && nodeData.status === "Disconnected") {
       node[nodeData.nodeId] = nodeData;
    }
//IF IT'S THERE AND THE STATUS OF THE NEXT ROW IS CONNECTED & THE SEQUENCE ID IS MORE THAN THE STORE SEQUENCE I.E THE NEXT RECORD
    else if ((nodeData.nodeId in node) && nodeData.status === "Connected" && nodeData.sequenceId > node[nodeData.nodeId].sequenceId) {
        var start_date = moment(node[nodeData.nodeId].date, 'DD-MM-YYYY');
        var end_date = moment(nodeData.date, 'DD-MM-YYYY');

//GETTING THE DIFFERENCE BETWEEN BOTH THE DATES AND BASED ON THE NO OF DAYS PUSHING IT TO THE RESULT OBJECT
        var diff = end_date.diff(start_date,'days');
        if(diff > 0) {
//FOR 1 OR MORE DAY DIFFERENCE
            for(var i = 1; i <= diff; i++) {
                var days = moment(node[nodeData.nodeId].date, 'DD-MM-YYYY').add(i, 'days');
                result[nodeData.nodeId].add(days.format())
            }
        }
        result[nodeData.nodeId].add(moment(node[nodeData.nodeId].date, 'DD-MM-YYYY').format());
//TO MOVE AHEAD SINCE THERE ARE MULTIPLE ROWS FOR EACH NODEID REMOVING THAT KEY
        delete node[nodeData.nodeId];
    }
        });
//LOGGING THE RESULT
        console.log(result)
});
 
// READING THE INPUT FILE AND SENDING DATA TO THE PARSER
fs.createReadStream(inputFile).pipe(parser);