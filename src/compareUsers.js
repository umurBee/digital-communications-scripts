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

// Prepare the summary file
const summaryFilePath = `${resultsDir}/summary.csv`;
let summaryStream = fs.createWriteStream(summaryFilePath);
summaryStream.write(
  'country,totalMongoAccounts,reachableAccounts,unreachableAccounts,reachabilityRate,emailAvailableAccounts,phoneAvailableAccounts,inAppAvailableAccounts,pushAvailableAccounts\n'
);
console.log(`Created summary file at ${summaryFilePath}`);
let summaryData = [];

// Iterate through all countries in countries.js
for (const countryCode in countries) {
  const country = countries[countryCode];

  const mongoFilePath = `exports/${formattedDate}/${countryCode}/mongo-processed/mongo-valid-users.csv`;
  const brazeFilePath = `exports/${formattedDate}/${countryCode}/braze-processed/braze-valid-users.csv`;
  const resultFilePath = `${resultsDir}/${countryCode}-result.csv`;

  // Check if the input files exist
  if (!fs.existsSync(mongoFilePath) || !fs.existsSync(brazeFilePath)) {
    console.error(`Files for ${countryCode} not found! Skipping...`);
    continue;
  }

  console.log(`Processing files for ${countryCode}:`);
  console.log(`Mongo: ${mongoFilePath}`);
  console.log(`Braze: ${brazeFilePath}`);

  processFiles(countryCode, mongoFilePath, brazeFilePath, resultFilePath);
}

// Ensure this runs after all countries are processed
setTimeout(() => {
  if (summaryData.length === 0) {
    console.log('No data to write in summary file.');
    return;
  }

  // Sort by reachability rate in descending order
  summaryData.sort((a, b) => b.reachabilityRate - a.reachabilityRate);

  // Write sorted data to the summary file
  let summaryStream = fs.createWriteStream(summaryFilePath);
  summaryStream.write(
    'country,totalMongoAccounts,reachableAccounts,unreachableAccounts,reachabilityRate,emailAvailableAccounts,phoneAvailableAccounts,inAppAvailableAccounts,pushAvailableAccounts\n'
  );

  summaryData.forEach(entry => {
    summaryStream.write(
      `${entry.countryCode},${entry.totalMongoAccounts},${entry.totalReachableAccounts},${entry.totalUnreachableAccounts},${entry.reachabilityRate},${entry.emailAvailable},${entry.phoneAvailable},${entry.inAppAvailable},${entry.pushAvailable}\n`
    );
  });

  summaryStream.end();
  console.log('Final summary file written successfully.');
}, 5000);


function processFiles(countryCode, mongoFilePath, brazeFilePath, resultFilePath) {
  const mongoAccounts = new Map(); // Store unique accountIds and counts from Mongo
  const reachableAccounts = new Set();
  const emailAvailableAccounts = new Set();
  const phoneAvailableAccounts = new Set();
  const inAppAvailableAccounts = new Set();
  const pushAvailableAccounts = new Set();

  // Read Mongo file
  fs.createReadStream(mongoFilePath)
    .pipe(csv())
    .on('data', (row) => {
      const valueUserId = row.value_userId;
      if (!valueUserId) return;

      // Extract the first portion (accountId before '_')
      const accountId = valueUserId.split('_')[0];
      if (!mongoAccounts.has(accountId)) {
        mongoAccounts.set(accountId, 0);
      }
      mongoAccounts.set(accountId, mongoAccounts.get(accountId) + 1);
    })
    .on('end', () => {
      console.log(`Mongo accounts loaded for ${countryCode}: ${mongoAccounts.size}`);
      processBrazeFile(countryCode, mongoAccounts, brazeFilePath, resultFilePath, {
        reachableAccounts,
        emailAvailableAccounts,
        phoneAvailableAccounts,
        inAppAvailableAccounts,
        pushAvailableAccounts,
      });
    });
}

function processBrazeFile(countryCode, mongoAccounts, brazeFilePath, resultFilePath, trackingSets) {
  const {
    reachableAccounts,
    emailAvailableAccounts,
    phoneAvailableAccounts,
    inAppAvailableAccounts,
    pushAvailableAccounts,
  } = trackingSets;

  const brazeUserCounts = new Map();

  fs.createReadStream(brazeFilePath)
    .pipe(csv())
    .on('data', (row) => {
      const externalId = row.external_id;
      if (!externalId) return;

      // Extract first portion (accountId before '_')
      const accountId = externalId.split('_')[0];

      if (!brazeUserCounts.has(accountId)) {
        brazeUserCounts.set(accountId, {
          brazeUserCount: 0,
          emailReachableCount: 0,
          phoneReachableCount: 0,
          inAppReachableCount: 0,
          pushReachableCount: 0,
          reachableCount: 0,
        });
      }

      const accStats = brazeUserCounts.get(accountId);
      accStats.brazeUserCount++;

      const isEmailAvailable = row.emailAvailable === 'TRUE';
      const isPhoneAvailable = row.phoneAvailable === 'TRUE';
      const isInAppAvailable = row.inAppAvailable === 'TRUE';
      const isPushAvailable = row.pushAvailable === 'TRUE';

      if (isEmailAvailable || isPhoneAvailable || isInAppAvailable || isPushAvailable) {
        accStats.reachableCount++;
        reachableAccounts.add(accountId);
      }
      if (isEmailAvailable) {
        accStats.emailReachableCount++;
        emailAvailableAccounts.add(accountId);
      }
      if (isPhoneAvailable) {
        accStats.phoneReachableCount++;
        phoneAvailableAccounts.add(accountId);
      }
      if (isInAppAvailable) {
        accStats.inAppReachableCount++;
        inAppAvailableAccounts.add(accountId);
      }
      if (isPushAvailable) {
        accStats.pushReachableCount++;
        pushAvailableAccounts.add(accountId);
      }
    })
    .on('end', () => {
      console.log(`Braze data processed for ${countryCode}`);
      updateResultsFile(countryCode, mongoAccounts, brazeUserCounts, resultFilePath);
      updateSummaryFile(countryCode, mongoAccounts, trackingSets);
    })
    .on('error', (err) => {
      console.error(`Error processing Braze file for ${countryCode}:`, err);
    });
}

function updateResultsFile(countryCode, mongoAccounts, brazeUserCounts, resultFilePath) {
  const accountStats = new Map();

  // Initialize with Mongo data
  mongoAccounts.forEach((count, accountId) => {
    accountStats.set(accountId, {
      userCount: count,
      brazeUserCount: 0,
      emailReachableCount: 0,
      phoneReachableCount: 0,
      inAppReachableCount: 0,
      pushReachableCount: 0,
      reachableCount: 0,
    });
  });

  // Merge Braze data
  brazeUserCounts.forEach((brazeStats, accountId) => {
    if (accountStats.has(accountId)) {
      const mongoData = accountStats.get(accountId);
      mongoData.brazeUserCount = brazeStats.brazeUserCount;
      mongoData.emailReachableCount = brazeStats.emailReachableCount;
      mongoData.phoneReachableCount = brazeStats.phoneReachableCount;
      mongoData.inAppReachableCount = brazeStats.inAppReachableCount;
      mongoData.pushReachableCount = brazeStats.pushReachableCount;
      mongoData.reachableCount = brazeStats.reachableCount;
    } else {
      accountStats.set(accountId, {
        userCount: 0,
        brazeUserCount: brazeStats.brazeUserCount,
        emailReachableCount: brazeStats.emailReachableCount,
        phoneReachableCount: brazeStats.phoneReachableCount,
        inAppReachableCount: brazeStats.inAppReachableCount,
        pushReachableCount: brazeStats.pushReachableCount,
        reachableCount: brazeStats.reachableCount,
      });
    }
  });

  // Write new results file
  const resultStream = fs.createWriteStream(resultFilePath);
  resultStream.write(
    'value,userCount,brazeUserCount,emailReachableCount,phoneReachableCount,inAppReachableCount,pushReachableCount,reachableCount\n'
  );

  accountStats.forEach((stats, accountId) => {
    resultStream.write(
      `${accountId},${stats.userCount},${stats.brazeUserCount},${stats.emailReachableCount},${stats.phoneReachableCount},${stats.inAppReachableCount},${stats.pushReachableCount},${stats.reachableCount}\n`
    );
  });

  resultStream.end();
  console.log(`Updated results file for ${countryCode}: ${resultFilePath}`);
}

function updateSummaryFile(countryCode, mongoAccounts, trackingSets) {
  const {
    reachableAccounts,
    emailAvailableAccounts,
    phoneAvailableAccounts,
    inAppAvailableAccounts,
    pushAvailableAccounts,
  } = trackingSets;

  const totalMongoAccounts = mongoAccounts.size;
  const totalReachableAccounts = reachableAccounts.size;
  const totalUnreachableAccounts = totalMongoAccounts - totalReachableAccounts;
  const reachabilityRate = totalMongoAccounts > 0 ? (totalReachableAccounts / totalMongoAccounts).toFixed(4) : 0;

  summaryData.push({
    countryCode,
    totalMongoAccounts,
    totalReachableAccounts,
    totalUnreachableAccounts,
    reachabilityRate,
    emailAvailable: emailAvailableAccounts.size,
    phoneAvailable: phoneAvailableAccounts.size,
    inAppAvailable: inAppAvailableAccounts.size,
    pushAvailable: pushAvailableAccounts.size,
  });
}

