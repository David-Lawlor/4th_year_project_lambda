var moment = require("moment");
var AWS = require("aws-sdk");

module.exports.handler = function(event, context) {
    
    var queryDateMonth = event.queryDateMonth;
    var queryDateDay = event.queryDateDay;
    var queryTable = event.table;
    var tableExtension = event.tableExt;
    var sensorAttribute = event.sensorAttribute;
    var tempid = event.id;
    var sensorLocation = event.sensorLocation;
    
    

    console.log(event);
    var data = [];


    var docClient = new AWS.DynamoDB.DocumentClient();

    var monthQuery = createQuery(queryDateMonth, queryTable, tableExtension, sensorAttribute, tempid);
    var dayQuery = createQuery(queryDateDay, queryTable, tableExtension, sensorAttribute, tempid);



    docClient.query(dayQuery, function(err, dayData) {
        if (err) {
            console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
        } else {
            var filteredData = filterByLocation(dayData, sensorLocation);
            console.log("Query succeeded.");
            var currentTemperature = filteredData[filteredData.length -1];
            var processedDayData = processDataDay(filteredData, sensorAttribute);

            docClient.query(monthQuery, function(err, monthData) {
                if (err) {
                    console.log("Unable to query. Error:", JSON.stringify(err, null, 2));
                } else {
                    var filteredData = filterByLocation(monthData, sensorLocation);
                    //console.log(filteredData);
                    console.log("Query succeeded.");
                    var processMonthData = processDataMonth(filteredData, sensorAttribute);
                    var returnData = {
                        Day: processedDayData,
                        Month: processMonthData,
                        CurrentReading: currentTemperature
                    };
                    //sendResponse(res, 200, returnData);
                    context.succeed(returnData);
                }
            });
        }
    });


};

var processDataDay =  function(data, sensorAttribute){
    console.log("processing data by day");

    // get the length of the array for the loop
    var arrayLength = data.length;

// create the arrays for the graph data and labels
    var graphData = [];
    var graphLabels = [];

// parse the hour from the first item in the array of objects
    var hour = parseHour(data[0].Time);

// create the graph label
    graphLabels.push(hour + ":00");

// initialise the totals
    var hourTotal = 0;
    var numberOfEntriesForHour = 0;

// loop through each object in the array
    for(var i = 0; i < arrayLength; i++){
        if (hour === parseHour(data[i].Time)) {
            hourTotal += parseFloat(data[i][sensorAttribute]);
            numberOfEntriesForHour++;
            hour = parseHour(data[i].Time);
        }
        else {
            graphData.push(Math.round((hourTotal / numberOfEntriesForHour)*100)/100);
            hourTotal = parseFloat(data[i][sensorAttribute]);
            numberOfEntriesForHour = 1;
            hour = parseHour(data[i].Time);
            if (hour === 0){
                graphLabels.push(hour + "00:00");
            }
            else {
                graphLabels.push(hour + ":00");
            }
        }

        // at the end of the array the hour will not change again so get average now
        if(i == arrayLength-1){
            graphData.push(Math.round((hourTotal / numberOfEntriesForHour)*100)/100);
        }
    }

    return  {
        "graphData": graphData,
        "graphLabels": graphLabels
    };
};

var processDataMonth =  function(data, sensorAttribute){
    console.log("processing data by day");

    // get the length of the array for the loop
    var arrayLength = data.length;

// create the arrays for the graph data and labels
    var graphData = [];
    var graphLabels = [];

    // parse the hour from the first item in the array of objects
    var day = parseDay(data[0].Date);

    // create the graph label
    graphLabels.push(data[0].Date);

    // initialise the totals
    var dayTotal = 0;
    var numberOfEntriesForDay = 0;

    // loop through each object in the array
    for(var i = 0; i < arrayLength; i++){
        if (day === parseDay(data[i].Date)) {
            dayTotal += parseFloat(data[i][sensorAttribute]);
            numberOfEntriesForDay++;
            day = parseDay(data[i].Date);
        }
        else {
            graphData.push(Math.round((dayTotal / numberOfEntriesForDay)*100)/100);
            dayTotal = parseFloat(data[i][sensorAttribute]);
            numberOfEntriesForDay = 1;
            day = parseDay(data[i].Date);
            graphLabels.push(data[i].Date);
        }
        // at the end of the array the hour will not change again so get average now
        if(i == arrayLength-1){
            graphData.push(Math.round((dayTotal / numberOfEntriesForDay)*100)/100);
        }
    }

    return  {
        "graphData": graphData,
        "graphLabels": graphLabels
    };
};


var sendResponse = function (res, status, content) {
    res.status(status);
    res.json(content);
};



var filterByLocation = function(data, sensorLocation){
    var filteredData = [];
    data.Items.forEach(function(item){
        //console.log(item);
        if(item.Location == sensorLocation){
            filteredData.push(item);
        }
    });
    return filteredData;
};

var createQuery = function (queryDate, queryTable, tableExtension, sensorAttribute, tempid) {
    return {
        TableName: queryTable,
        ProjectionExpression: "#d, #t, #attr , #l",
        KeyConditionExpression: "#pk = :Partition_Key and #ts > :queryDate",
        ExpressionAttributeNames: {
            "#pk": "Partition_Key",
            "#ts": "Timestamp",
            "#d": "Date",
            "#t": "Time",
            "#attr": sensorAttribute,
            "#l": "Location"
        },
        ExpressionAttributeValues: {
            ":Partition_Key": tempid + tableExtension,
            ":queryDate": queryDate
        }
    };
};


var parseHour = function(time){
    return parseInt(time.substring(0, 2));
};

var parseDay = function(date){
    return parseInt(date.substring(0, 2));
};

var parseMonth = function(date){
    return parseInt(date.substring(3, 5));
};

var parseYear = function(date){
    return parseInt(date.substring(6, 10));
};




