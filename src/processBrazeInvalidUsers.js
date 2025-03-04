const fs = require('fs');
const path = require('path');
const countries = require('./countries');  // Import countries configuration

const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

// Ensure the base export folder exists
const baseExportFolder = path.join(__dirname, '..', 'exports');
ensureFolderExists(baseExportFolder);

async function processInvalidUsers(countryCode) {
  // Dynamically define the folder paths for each country
  const rawFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-raw');
  const processedFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-processed');

  // Ensure the processed folder exists
  ensureFolderExists(processedFolderPath);

  const invalidUsersFolderPath = path.join(rawFolderPath, 'invalidUsers');
  const processedFilePath = path.join(processedFolderPath, 'braze-invalid-users.csv');

  // Ensure country-specific folder exists
  ensureFolderExists(path.join(processedFolderPath));

  const files = await getTxtFiles(invalidUsersFolderPath);
  if (files.length === 0) {
    console.log(`No .txt files found in ${countryCode}/invalidUsers folder.`);
    return;
  }

  let brazeIds = [];

  for (const file of files) {
    const filePath = path.join(invalidUsersFolderPath, file);
    const fileData = await readFile(filePath);
    const ids = extractBrazeIds(fileData);
    brazeIds = [...brazeIds, ...ids];
  }

  // Manually write the CSV with the header and data
  const header = 'braze_id\n';
  const rows = brazeIds.map(id => id.braze_id).join('\n');
  const csvContent = header + rows;

  // Ensure the file path folder exists
  ensureFolderExists(path.dirname(processedFilePath));

  fs.writeFileSync(processedFilePath, csvContent, 'utf8');
  console.log(`Processed CSV file saved to ${processedFilePath}`);
}

function ensureFolderExists(folderPath) {
  if (!fs.existsSync(folderPath)) {
    fs.mkdirSync(folderPath, { recursive: true });
  }
}

function getTxtFiles(folderPath) {
  return new Promise((resolve, reject) => {
    fs.readdir(folderPath, (err, files) => {
      if (err) {
        reject(err);
        return;
      }
      const txtFiles = files.filter(file => file.endsWith('.txt'));
      resolve(txtFiles);
    });
  });
}

function readFile(filePath) {
  return new Promise((resolve, reject) => {
    fs.readFile(filePath, 'utf8', (err, data) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(data);
    });
  });
}

function extractBrazeIds(data) {
  const lines = data.split('\n');
  return lines
    .filter(line => line.trim() !== '')
    .map(line => {
      const parsed = JSON.parse(line.trim());
      return { braze_id: parsed.braze_id };
    });
}

// Iterate through all countries in countries.js and process invalid users for each
async function processAllCountries() {
  const countryCodes = Object.keys(countries);

  for (const countryCode of countryCodes) {
    console.log(`Processing invalid users for country: ${countryCode}`);
    await processInvalidUsers(countryCode);
  }
}

// Execute the process for all countries
processAllCountries().catch(err => {
  console.error('Error during processing:', err);
});
