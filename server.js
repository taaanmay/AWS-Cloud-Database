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
app.get('/createTableFunc', setup_table);
app.get('/deleteTableFunc', delete_table);
app.get('/queryTableFunc/:year', query_dynamo_table); 
app.get('/queryTableFunc/:year/:rating/:prefix', query_dynamo_table);
app.get('/queryTableFunc/:year/:rating', query_dynamo_table);

app.listen(port, () => console.log(`App listening on port ${port}`));

const S3_OBJECT = 'moviedata.json';
const S3_BUCKET = 'csu44000assignment220';
const DB_TABLE = 'movies';

    
let s3 = new AWS.S3();
let dynamoDB = new AWS.DynamoDB();

const BATCH_SIZE = 25;


// Function to check if table exists or not
async function doesDynamoTableExist(tableName) {
    let data = await dynamoDB.listTables({}).promise();
    
    return data.TableNames.includes(tableName);
}


// Function to generate Response messages
function send_response(status, text, movies) {
    return { result: {
        status: status,
        text: text,
        movies: movies
    }};
}



// ======================== SET-UP TABLE ========================================
// Creating a DynamoDB Table and filling it with movies data
async function setup_table(_, res) {
    // Check if table exists
    let tableExists = await doesDynamoTableExist(DB_TABLE);
    if (tableExists) {
        res.json(send_response(false, 'Table already exists', {}));
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
    
    res.json(send_response(true, 'Creation successful!', {}));
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
            { AttributeName: 'title_upper_case', AttributeType: 'S' },
            { AttributeName: 'releaseYear', AttributeType: 'N' },
            // { AttributeName: 'rating', AttributeType: 'N' },
        ],
        KeySchema: [
            { AttributeName: 'title_upper_case', KeyType: 'HASH' },
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
    let list_of_movies = [];
    let all_movies_set = [];
    
    // To use Batch Write Item, we send use batches of movie with the size 25. 
    // Add the list_of_movies and when it reaches the capacity of 25, add it to all_movies_set
    let index;
    for (index = 0;index < json.length; index++) {

        // If capacity is reached, push movies to all_movies_set
        if (list_of_movies.length == BATCH_SIZE) {
            all_movies_set.push(list_of_movies);
            list_of_movies = [];
        }

        // Add Movie to the set with params
        list_of_movies.push({
            PutRequest: {
                Item: {
                    title_upper_case: {'S': json[index].title.toUpperCase() },
                    releaseYear: {'N': json[index].year?.toString() ?? '-1' },
                    title: {'S': json[index].title },
                    rating: {'N': json[index].info.rating?.toString() ?? '-1' },
                    rank: {'N': json[index].info.rank?.toString() ?? '-1' }
                }
            }
        });
    }

    // Push the movies if any left
    if (list_of_movies.length != 0) all_movies_set.push(list_of_movies);
    

    // Use Batch Write function to upload to DynamoDB
    for (var i = 0; i < all_movies_set.length; i++) {
        console.log(`${i + 1} batch uploaded`);
       
        await dynamoDB.batchWriteItem({ RequestItems: { [tableName]: all_movies_set[i] } }).promise();
    }
    
}


// ======================== DELETE TABLE ========================================


// Function to delete DynamoDB table
async function delete_table(_, res) {
    let tableExists = await doesDynamoTableExist(DB_TABLE);
    
    if (!tableExists) {
        res.json(send_response(false, 'Table does not exist.', {}));
        return;
    }
    
    console.log('Deleting Table')
    let params = { TableName: DB_TABLE };
    await dynamoDB.deleteTable(params).promise();
    console.log('Done')
    
    res.json(send_response(true, 'Table Deleted!', {}));
}


// ======================== QUERY TABLE ========================================

// Function to query movies using Year, Prefix and Rating
async function query_dynamo_table(req, res) {
    
    // Extract Parameters from URL
    let year = parseInt(req.params.year, 10);
    let ratingParam = parseInt(req.params.rating, 10) ?? 0;
    let prefix = req.params.prefix ?? '';
    

    if (isNaN(year)) {
        // Year not entered
        res.json(send_response(false, 'Invalid year', {}));
    } else {

        if(isNaN(ratingParam)){
            ratingParam = 0;
        }
        console.log('Querying Database Table');
        let data = await get_data(DB_TABLE, year.toString(), prefix.toUpperCase(), ratingParam.toString());
        console.log('Querying Done');
        res.json(send_response(true, 'OK', data));
    }
}


// Function Querying the DynamoDB table
async function get_data(tableName, year, prefix, rating) {
    // Parameter Defined
    let params = {
        TableName: tableName,
        ExpressionAttributeValues: {
            ':y': {N: year},
            ':p': {S: prefix},
            ':r': {N: rating}
        },
        FilterExpression: 'releaseYear = :y and begins_with (title_upper_case, :p) and rating >= :r',
        ProjectionExpression: 'title, releaseYear, rating'
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
        });
    });

    return data;
}


