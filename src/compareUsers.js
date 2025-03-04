const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const countries = require('./countries'); // Import countries.js

// Get the current date in YYYY-MM-DD format
const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

// Prepare the results folder
const resultsDir = `results/${formattedDate}`;
if (!fs.existsSync(resultsDir)) {
  fs.mkdirSync(resultsDir, { recursive: true });
  console.log(`Created results directory: ${resultsDir}`);
}

// Prepare the summary file path
const summaryFilePath = `${resultsDir}/summary.csv`;

// Check if the summary file exists
let summaryStream;
if (!fs.existsSync(summaryFilePath)) {
  // If the file doesn't exist, create it and write the header
  summaryStream = fs.createWriteStream(summaryFilePath);
  summaryStream.write('country,mongousercount,reachablecount,reachablebyPhonecount,reachablebyEmailcount,reachablebyPushcount,reachablebyInAppCount\n');
  console.log(`Created summary file at ${summaryFilePath}`);
} else {
  // If the file exists, open it in append mode
  summaryStream = fs.createWriteStream(summaryFilePath, { flags: 'a' });
}

// Iterate through all countries in countries.js
for (const countryCode in countries) {
  const country = countries[countryCode];

  const mongoFilePath = `exports/${formattedDate}/${countryCode}/mongo-processed/mongo-valid-users.csv`;
  const brazeFilePath = `exports/${formattedDate}/${countryCode}/braze-processed/braze-valid-users.csv`;

  // Check if the input files exist
  if (!fs.existsSync(mongoFilePath) || !fs.existsSync(brazeFilePath)) {
    console.error(`Files for ${countryCode} not found! Skipping...`);
    continue;
  }

  console.log(`Processing files for ${countryCode}:`);
  console.log(`Mongo: ${mongoFilePath}`);
  console.log(`Braze: ${brazeFilePath}`);

  // Create writable streams for result files
  const resultStream = fs.createWriteStream(`${resultsDir}/${countryCode}-result.csv`);
  const invalidStream = fs.createWriteStream(`${resultsDir}/${countryCode}-braze-invalid-users.csv`);

  // Write the headers to the result files
  resultStream.write('external_id,emailAvailable,phoneAvailable,inAppAvailable,pushAvailable,Reachable\n');
  invalidStream.write('external_id\n'); // Only external_id for invalid users

  // Read Mongo CSV and populate the set with user IDs
  const mongoUserIds = new Set();
  let mongoUserCount = 0; // Counter for Mongo rows
  fs.createReadStream(mongoFilePath)
    .pipe(csv())
    .on('data', (row) => {
      mongoUserIds.add(row.value_userId);
      mongoUserCount++;
    })
    .on('end', () => {
      console.log(`Mongo users for ${countryCode} loaded. Total rows: ${mongoUserCount}`);
      processBrazeFile(countryCode, mongoUserIds, brazeFilePath, resultStream, invalidStream, mongoUserCount);
    });
}

function processBrazeFile(countryCode, mongoUserIds, brazeFilePath, resultStream, invalidStream, mongoUserCount) {
  const invalidBrazeUsers = [];
  let brazeUserCount = 0; // Counter for Braze rows
  let validUserCount = 0; // Counter for valid users written to resultStream
  let invalidUserCount = 0; // Counter for invalid users written to invalidStream

  const brazeUsers = {}; // Store Braze users by their external_id
  let reachableCount = 0;
  let reachableByPhoneCount = 0;
  let reachableByEmailCount = 0;
  let reachableByPushCount = 0;
  let reachableByInAppCount = 0;  // New count for inApp users

  fs.createReadStream(brazeFilePath)
    .pipe(csv())
    .on('data', (row) => {
      brazeUserCount++;
      const mongoUserId = row.external_id;
      brazeUsers[mongoUserId] = row; // Store the Braze user by external_id
    })
    .on('end', () => {
      console.log(`Braze users for ${countryCode} processed. Total rows: ${brazeUserCount}`);

      // First, upload all Mongo users with N/A channels and calculate reachability
      mongoUserIds.forEach((mongoUserId) => {
        const brazeUser = brazeUsers[mongoUserId];
        let emailAvailable = 'N/A';
        let phoneAvailable = 'N/A';
        let inAppAvailable = 'N/A';
        let pushAvailable = 'N/A';
        let reachable = 'FALSE'; // Default reachable value

        // If the Mongo user exists in Braze, populate TRUE/FALSE values
        if (brazeUser) {
          emailAvailable = brazeUser.emailAvailable;
          phoneAvailable = brazeUser.phoneAvailable;
          inAppAvailable = brazeUser.inAppAvailable;
          pushAvailable = brazeUser.pushAvailable;

          // Update reachable if any channel is TRUE
          reachable =
            emailAvailable === 'TRUE' ||
            phoneAvailable === 'TRUE' ||
            inAppAvailable === 'TRUE' ||
            pushAvailable === 'TRUE'
              ? 'TRUE'
              : 'FALSE';
        }

        // Update counts for reachability
        if (reachable === 'TRUE') {
          reachableCount++;
        }
        if (phoneAvailable === 'TRUE') {
          reachableByPhoneCount++;
        }
        if (emailAvailable === 'TRUE') {
          reachableByEmailCount++;
        }
        if (pushAvailable === 'TRUE') {
          reachableByPushCount++;
        }
        if (inAppAvailable === 'TRUE') {  // Count users reachable by InApp
          reachableByInAppCount++;
        }

        // Write the Mongo user to the result file
        resultStream.write(`${mongoUserId},${emailAvailable},${phoneAvailable},${inAppAvailable},${pushAvailable},${reachable}\n`);
        validUserCount++;
      });

      // Now, look for invalid Braze users (those who are in Braze but not in Mongo)
      for (const brazeUserId in brazeUsers) {
        if (!mongoUserIds.has(brazeUserId)) {
          invalidBrazeUsers.push(brazeUserId);
          invalidUserCount++;
        }
      }

      // Write invalid Braze users to the invalid file
      invalidBrazeUsers.forEach((userId) => {
        invalidStream.write(`${userId}\n`);
      });

      // Close the writable streams after all data is written
      resultStream.end(() => {
        console.log(`${countryCode}-result.csv written successfully.`);
      });
      invalidStream.end(() => {
        console.log(`${countryCode}-braze-invalid-users.csv written successfully.`);
      });

      console.log(`Files for ${countryCode} successfully saved.`);
      console.log(`Valid users written: ${validUserCount}`);
      console.log(`Invalid users written: ${invalidUserCount}`);

      // Write to the daily summary report
      summaryStream.write(
        `${countryCode},${mongoUserCount},${reachableCount},${reachableByPhoneCount},${reachableByEmailCount},${reachableByPushCount},${reachableByInAppCount}\n`
      );
    })
    .on('error', (err) => {
      console.error(`Error processing Braze file for ${countryCode}:`, err);
    });
}
