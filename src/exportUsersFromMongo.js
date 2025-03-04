require('dotenv').config(); // Load environment variables from .env file
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const csvWriter = require('csv-write-stream');
const path = require('path');
const countries = require('./countries'); // Importing the countries.js module

const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

// Function to introduce a delay
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Function to connect to MongoDB and fetch data
async function fetchData(countryCode) {
    try {
        const cluster = countries[countryCode].cluster;  // Get the cluster for the country
        let connectionString;

        // Select the correct connection string based on the cluster
        if (cluster.toLowerCase() === 'global') {
            connectionString = process.env.MONGO_GLOBAL_CLUSTER; // Read from .env for global cluster
        } else if (cluster.toLowerCase() === 'europe') {
            connectionString = process.env.MONGO_EUROPE_CLUSTER; // Read from .env for europe cluster
        }

        if (!connectionString) {
            throw new Error(`No connection string found for cluster: ${cluster}`);
        }

        // Connect to the database
        const client = await MongoClient.connect(connectionString);
        const db = client.db('identity-metadata');
        const collectionName = `user-metadata-${countryCode}`;  // Collection name based on the country code
        const collection = db.collection(collectionName);

        // Apply the filter
        const filter = { type: "ACCOUNT" };

        // Fetch the data (only _id, userId, and value fields)
        const data = await collection.find(filter).project({ _id: 1, userId: 1, value: 1 }).toArray();

        // Close the connection
        client.close();

        return data;
    } catch (error) {
        console.error(`Error fetching data for ${countryCode}:`, error);
    }
}

// Function to write data to CSV
function writeToCSV(data, countryCode) {
    // Define the file path
    const folderPath = path.join('exports', formattedDate, countryCode, 'mongo-raw');
    fs.mkdirSync(folderPath, { recursive: true }); // Ensure directory exists

    const writer = csvWriter({ headers: ['_id', 'userId', 'value'] });
    const fileName = `user-metadata-${countryCode}.csv`;

    writer.pipe(fs.createWriteStream(path.join(folderPath, fileName)));
    data.forEach(row => {
        writer.write({
            _id: row._id,
            userId: row.userId,
            value: row.value,
        });
    });
    writer.end();

    console.log(`Data for ${countryCode} written to ${fileName}`);
}

// Main function to process all countries with a delay
async function processCountries() {
    for (const countryCode in countries) {
        console.log(`Processing country: ${countryCode}`);
        const data = await fetchData(countryCode);

        if (data && data.length > 0) {
            writeToCSV(data, countryCode);
        } else {
            console.log(`No data found for ${countryCode}`);
        }

        console.log(`Waiting 3 minutes before processing the next country...`);
        await delay(180000); // Wait for 3 minutes
    }
}

// Run the script
processCountries();
