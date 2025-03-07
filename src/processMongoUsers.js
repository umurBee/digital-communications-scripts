const fs = require('fs');
const path = require('path');
const countries = require('./countries');  // Import countries configuration
const readline = require('readline');  // Use readline to stream the files line by line

// Get the current date in ISO format (YYYY-MM-DD)
const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

// Define the base export and results folders
const baseExportFolder = path.join(__dirname, '..', 'exports');
const resultsFolder = path.join(__dirname, '..', 'results', formattedDate);

// Ensure the results folder exists
ensureFolderExists(resultsFolder);

// Function to process the user metadata CSV file for a given country
async function processUserMetadata(countryCode) {
  // Dynamically set the paths for raw and processed folders based on the country code and date
  const rawFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'mongo-raw');
  const processedFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'mongo-processed');

  // Ensure the processed folder exists
  ensureFolderExists(processedFolderPath);

  const inputFilePath = path.join(rawFolderPath, `user-metadata-${countryCode}.csv`);
  const resultFilePath = path.join(resultsFolder, `${countryCode}-result.csv`);
  const outputFilePath = path.join(processedFolderPath, 'mongo-valid-users.csv');

  if (!fs.existsSync(inputFilePath)) {
    console.log(`No CSV file found for country: ${countryCode}`);
    return;
  }

  // Read the input CSV file line by line
  const rl = readline.createInterface({
    input: fs.createReadStream(inputFilePath),
    output: process.stdout,
    terminal: false
  });

  const valueUserMap = new Map();  // Store unique values with a set of userIds
  const concatenatedData = ['value_userId'];  // Store concatenated value_userId pairs

  let isHeader = true; // Skip the header
  rl.on('line', (line) => {
    if (isHeader) {
      isHeader = false;
      return; // Skip the first line (header)
    }

    const parts = line.split(','); // Input file uses commas as delimiters
    if (parts.length < 3) return; // Ensure we have at least 3 columns (_id, userId, value)

    const [, userId, value] = parts.map(item => item.trim()); // Trim whitespace

    // Store concatenated value_userId for the second CSV
    concatenatedData.push(`${value}_${userId}`);

    // Store userId count per value
    if (!valueUserMap.has(value)) {
      valueUserMap.set(value, new Set());
    }
    valueUserMap.get(value).add(userId);
  });

  rl.on('close', () => {
    // Prepare the data for value-user count result file
    const resultData = ['value,userCount'];
    valueUserMap.forEach((userIds, value) => {
      resultData.push(`${value},${userIds.size}`);
    });

    // Write the processed data to the results folder
    fs.writeFileSync(resultFilePath, resultData.join('\n'), 'utf8');
    console.log(`Result CSV file saved to ${resultFilePath}`);

    // Write the concatenated data to the mongo-valid-users.csv file
    fs.writeFileSync(outputFilePath, concatenatedData.join('\n'), 'utf8');
    console.log(`Processed valid users CSV file saved to ${outputFilePath}`);
  });
}

// Ensure folder exists
function ensureFolderExists(folderPath) {
  if (!fs.existsSync(folderPath)) {
    fs.mkdirSync(folderPath, { recursive: true });
  }
}

// Process the metadata for each country
async function processAllCountries() {
  const countryCodes = Object.keys(countries);  // Fetch country codes dynamically from countries.js
  for (const countryCode of countryCodes) {
    console.log(`Processing user metadata for country: ${countryCode}`);
    await processUserMetadata(countryCode);
  }
}

// Execute the process for all countries
processAllCountries().catch(err => {
  console.error('Error during processing:', err);
});
