const AWS = require('aws-sdk');
const config = require('./config.json');



const express = require('express');
const app = express();
const port = 3000;
const path = require('path');
let publicPath = path.resolve(__dirname, 'public');

AWS.config.update({
    accessKeyId: config.accessKeyId,
    secretAccessKey:config.secretAccessKey,
    region: config.region
});


app.use(express.static(publicPath));
app.get('/appCreate', setup_table);
app.get('/appDelete', delete_table);
app.get('/appQuery/:year', query_dynamo_table); // in case no prefix is included
// app.get('/appQuery/:year/:prefix', query_dynamo_table);

app.get('/appQuery/:year/:rating/:prefix', query_dynamo_table);
app.get('/appQuery/:year/:rating', query_dynamo_table);

app.listen(port, () => console.log(`App listening on port ${port}`));

const S3_BUCKET = 'csu44000assignment220';
const S3_OBJECT = 'moviedata.json';
const DB_TABLE = 'movies';
const BATCH_SIZE = 25;
    
let s3 = new AWS.S3();
let dynamoDB = new AWS.DynamoDB();


// Function to check if table exists or not
async function checkDynamoTableExists(tableName) {
    let data = await dynamoDB.listTables({}).promise();
    return data.TableNames.includes(tableName);
}


// Function to generate Response messages
function generateResponse(_success, _message, _movies) {
    return { result: {
        success: _success,
        message: _message,
        movies: _movies
    }};
}



// ======================== SET-UP TABLE ========================================
// Creating a DynamoDB Table and filling it with movies data
async function setup_table(_, res) {
    // Check if table exists
    let tableExists = await checkDynamoTableExists(DB_TABLE);
    if (tableExists) {
        res.json(generateResponse(false, 'Table already exists', {}));
        return;
    }

    // S3 Bucket
    console.log('Contacting S3 bucket')
    let json = await get_data_from_S3();
    console.log(json);
    console.log('Data from S3 bucket extracted successfully')
    
    
    // DynamoDB
    console.log('Creating Table');
    await create_dynamo_table(DB_TABLE);
    
    // Wait until the table is created
    await dynamoDB.waitFor('tableExists', { TableName: DB_TABLE }).promise(); 

    await insertIntoDynamoTable(DB_TABLE, json);
   
    console.log('Done!');
    
    res.json(generateResponse(true, 'Creation successful!', {}));
}

// Function to get data from S3 bucket
    // Used in setup_table function
async function get_data_from_S3() {
    let params = {
        Bucket: S3_BUCKET,
        Key: S3_OBJECT
    };
    let data = await s3.getObject(params).promise();
    return JSON.parse(data.Body.toString('utf-8'));
}

// Function to Create DynamoDB table
    // Used in setup_table function
async function create_dynamo_table(tableName) {
    let params = {
        AttributeDefinitions: [
            { AttributeName: 'titleLower', AttributeType: 'S' },
            { AttributeName: 'releaseYear', AttributeType: 'N' },
            // { AttributeName: 'rating', AttributeType: 'N' },
        ],
        KeySchema: [
            { AttributeName: 'titleLower', KeyType: 'HASH' },
            { AttributeName: 'releaseYear', KeyType: 'RANGE' },
            // { AttributeName: 'rating', KeyType: 'RANGE' }

        ],
        ProvisionedThroughput: {
            ReadCapacityUnits: 5,
            WriteCapacityUnits: 5
        },
        TableName: tableName
    };

    await dynamoDB.createTable(params).promise();
    // dynamoDB.createTable(params, function(err, data) {
    //     if (err) {
    //         console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
    //     } else {
    //         console.log("Table Created");
    //     }
    // });
}

// Function to insert data into DynamoDB table
    // Used in setup_table function
async function insertIntoDynamoTable(tableName, json) {
    // DynamoDB only allows batch inserts of 25 items
    let batches = [], batch = [];
    for (var i = 0; i < json.length; i++) {
        if (batch.length == BATCH_SIZE) {
            batches.push(batch);
            batch = [];
        }
        batch.push({
            PutRequest: {
                Item: {
                    titleLower: {'S': json[i].title.toLowerCase() },
                    releaseYear: {'N': json[i].year?.toString() ?? '-1' },
                    title: {'S': json[i].title },
                    rating: {'N': json[i].info.rating?.toString() ?? '-1' },
                    rank: {'N': json[i].info.rank?.toString() ?? '-1' }
                }
            }
        });
    }
    if (batch.length != 0) batches.push(batch);
    
    for (var i = 0; i < batches.length; i++) {
        console.log(`Inserting data batch ${i + 1}/${batches.length}`);
        await dynamoDB.batchWriteItem({ RequestItems: { [tableName]: batches[i] } }).promise();
    }
    
}



// ======================== DELETE TABLE ========================================


// Function to delete DynamoDB table
async function delete_table(_, res) {
    let tableExists = await checkDynamoTableExists(DB_TABLE);
    if (!tableExists) {
        res.json(generateResponse(false, 'Table does not exist.', {}));
        return;
    }
    
    console.log('Deleting Table')
    // await deleteDynamoTable(DB_TABLE);
    let params = { TableName: DB_TABLE };
    await dynamoDB.deleteTable(params).promise();
    console.log('Done')
    
    res.json(generateResponse(true, 'Table Deleted!', {}));
}



// Function to delete table
    
async function deleteDynamoTable(tableName) {
    let params = { TableName: tableName };
    await dynamoDB.deleteTable(params).promise();
}


// ======================== QUERY TABLE ========================================

// Function to query movies using Year, Prefix and Rating
async function query_dynamo_table(req, res) {
    let year = parseInt(req.params.year, 10);
    let prefix = req.params.prefix ?? '';
    let ratingParam = parseInt(req.params.rating, 10) ?? 0;

    if (isNaN(year)) {
        // Year not entered
        res.json(generateResponse(false, 'Invalid year', {}));
    } else {

        if(isNaN(ratingParam)){
            ratingParam = 0;
        }
        console.log('Querying Database Table');
        let data = await get_data(DB_TABLE, year.toString(), prefix.toLowerCase(), ratingParam.toString());
        console.log('Querying Done');
        res.json(generateResponse(true, 'OK', data));
    }
}


// Function Querying the DynamoDB table
async function get_data(tableName, year, prefix, rating) {
    // Parameter Defined
    let params = {
        ExpressionAttributeValues: {
            ':y': {N: year},
            ':p': {S: prefix},
            ':r': {N: rating}
        },
        FilterExpression: 'releaseYear = :y and begins_with (titleLower, :p) and rating >= :r',
        ProjectionExpression: 'title, releaseYear, rating',
        // ProjectionExpression: 'title, releaseYear, rating, rank',
        TableName: tableName
    }
    
    // Scan Data
    let raw = await dynamoDB.scan(params).promise();
    let data = [];

    // Iterate through item and push it to data
    raw.Items.forEach(function (item, _, _) {
        data.push({
            title: item.title.S,
            year: item.releaseYear.N,
            rating: item.rating.N
            // rank: item.rank.N
        });
    });

    return data;
}


