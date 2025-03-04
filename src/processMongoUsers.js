const fs = require('fs');
const path = require('path');
const countries = require('./countries');  // Import countries configuration
const readline = require('readline');  // Use readline to stream the files line by line

// Get the current date in ISO format (YYYY-MM-DD)
const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

// Define the base export folder
const baseExportFolder = path.join(__dirname, '..', 'exports');

// Function to process the user metadata CSV file for a given country
async function processUserMetadata(countryCode) {
  // Dynamically set the paths for raw and processed folders based on the country code and date
  const rawFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'mongo-raw');
  const processedFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'mongo-processed');

  // Ensure the processed folder exists
  ensureFolderExists(processedFolderPath);

  const inputFilePath = path.join(rawFolderPath, `user-metadata-${countryCode}.csv`);
  const outputFilePath = path.join(processedFolderPath, `mongo-valid-users.csv`);

  // Ensure country-specific folder exists
  ensureFolderExists(processedFolderPath);

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

  let outputData = [];  // Initialize with header

  rl.on('line', (line) => {
    if (line.trim()) {
      const [id, userId, value] = line.split(',');

      // Concatenate value and userId with an underscore
      const beesId = `${value}_${userId}`;
      outputData.push(beesId);
    }
  });

  rl.on('close', () => {
    // Write the processed data to the output CSV file
    fs.writeFileSync(outputFilePath, outputData.join('\n'), 'utf8');
    console.log(`Processed CSV file saved to ${outputFilePath}`);
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
