const fs = require('fs');
const path = require('path');
const countries = require('./countries');  // Import countries configuration
const readline = require('readline');  // Use readline to stream the files line by line

const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

// Ensure the base export folder exists
const baseExportFolder = path.join(__dirname, '..', 'exports');
ensureFolderExists(baseExportFolder);

// Load valid user IDs from mongo-valid-users.csv
async function loadValidMongoUsers(countryCode) {
  const mongoFilePath = path.join(baseExportFolder, formattedDate, countryCode, 'mongo-processed', 'mongo-valid-users.csv');
  
  if (!fs.existsSync(mongoFilePath)) {
    throw new Error(`Mongo valid users file not found for ${countryCode}: ${mongoFilePath}. Please run processMongoUsers.js first.`);
  }

  const validMongoUsers = new Set();
  const rl = readline.createInterface({
    input: fs.createReadStream(mongoFilePath),
    output: process.stdout,
    terminal: false
  });

  let firstLineSkipped = false;
  for await (const line of rl) {
    if (!firstLineSkipped) {
      firstLineSkipped = true; // Skip header
      continue;
    }
    validMongoUsers.add(line.trim());
  }

  return validMongoUsers;
}


// Process the validUsers folder and create a CSV template with external_id
async function processValidUsers(countryCode) {
  const rawFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-raw');
  const processedFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-processed');
  const unknownUsersFilePath = path.join(processedFolderPath, 'unknown-users-braze.csv'); // File for unknown users

  ensureFolderExists(processedFolderPath);

  const validUsersFolderPath = path.join(rawFolderPath, 'validUsers');
  const processedFilePath = path.join(processedFolderPath, `braze-valid-users.csv`);

  const files = await getTxtFiles(validUsersFolderPath);
  if (files.length === 0) {
    console.log(`No .txt files found in ${countryCode}/validUsers folder.`);
    return;
  }

  const validMongoUsers = await loadValidMongoUsers(countryCode);
  let externalIds = [];
  let unknownIds = [];

  // Stream and process files line by line
  for (const file of files) {
    const filePath = path.join(validUsersFolderPath, file);
    const ids = await streamFileAndExtractExternalIds(filePath);

    ids.forEach(id => {
      if (validMongoUsers.has(id)) {
        externalIds.push(id);
      } else {
        unknownIds.push(id);
      }
    });
  }

  // Write unknown externalIds to a separate file
  if (unknownIds.length > 0) {
    const unknownContent = unknownIds.join('\n') + '\n';
    fs.writeFileSync(unknownUsersFilePath, unknownContent, 'utf8');
    console.log(`Unknown users saved to ${unknownUsersFilePath}`);
  }

  if (externalIds.length === 0) {
    console.log(`No valid externalIds found in ${countryCode}, skipping further processing.`);
    return;
  }

  // Prepare the CSV content with header
  const header = 'external_id,emailAvailable,phoneAvailable,inAppAvailable,pushAvailable\n';
  const rows = externalIds.map(id => `${id},FALSE,FALSE,FALSE,FALSE`).join('\n');
  const csvContent = header + rows;

  // Write the initial CSV with only the valid external_ids
  fs.writeFileSync(processedFilePath, csvContent, 'utf8');
  console.log(`Processed CSV file saved to ${processedFilePath}`);

  // Update it with TRUE/FALSE values based on the other folders
  await updateUserDataWithAvailability(externalIds, countryCode, processedFilePath);
}

// Stream file and extract externalIds line by line
function streamFileAndExtractExternalIds(filePath) {
  return new Promise((resolve, reject) => {
    const externalIds = [];
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      output: process.stdout,
      terminal: false
    });

    rl.on('line', (line) => {
      if (line.trim()) {
        const parsed = JSON.parse(line.trim());
        externalIds.push(parsed.external_id);
      }
    });

    rl.on('close', () => resolve(externalIds));
    rl.on('error', (err) => reject(err));
  });
}

// Check if external_ids exist in other folders and update the final CSV
// Check if external_ids exist in other folders and update the final CSV
async function updateUserDataWithAvailability(externalIds, countryCode, processedFilePath) {
  const foldersToCheck = ['emailAvailableUsers', 'phoneAvailableUsers', 'inAppAvailableUsers', 'pushAvailableUsers'];

  let finalData = fs.readFileSync(processedFilePath, 'utf8').split('\n');
  
  // Update header to include 'Reachable' column
  finalData[0] = 'external_id,emailAvailable,phoneAvailable,inAppAvailable,pushAvailable,Reachable';

  // Use Promise.all() to process folders in parallel
  await Promise.all(foldersToCheck.map(async (folder) => {
    const folderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-raw', folder);
    console.log(`Checking folder: ${folderPath}`);

    const files = await getTxtFiles(folderPath);
    const availableIds = new Set();

    // Collect all external_ids in the folder by streaming files
    await Promise.all(files.map(async (file) => {
      const filePath = path.join(folderPath, file);
      const ids = await streamFileAndExtractExternalIds(filePath);
      ids.forEach(id => availableIds.add(id));
    }));

    console.log(`Found ${availableIds.size} IDs in ${folder}`);

    // Update the finalData rows for each external_id
    finalData = finalData.map((line, index) => {
      if (index === 0) return line; // Skip header

      const [external_id, emailAvailable, phoneAvailable, inAppAvailable, pushAvailable] = line.split(',');

      if (externalIds.includes(external_id) && availableIds.has(external_id)) {
        return [
          external_id,
          folder === 'emailAvailableUsers' ? 'TRUE' : emailAvailable,
          folder === 'phoneAvailableUsers' ? 'TRUE' : phoneAvailable,
          folder === 'inAppAvailableUsers' ? 'TRUE' : inAppAvailable,
          folder === 'pushAvailableUsers' ? 'TRUE' : pushAvailable,
        ].join(',');
      }
      return line;
    });
  }));

  // After updating channel availability, add Reachable column
  finalData = finalData.map((line, index) => {
    if (index === 0) return line; // Skip header

    const [external_id, emailAvailable, phoneAvailable, inAppAvailable, pushAvailable] = line.split(',');

    // Determine if at least one communication method is available
    const reachable = [emailAvailable, phoneAvailable, inAppAvailable, pushAvailable].includes('TRUE') ? 'TRUE' : 'FALSE';

    return `${external_id},${emailAvailable},${phoneAvailable},${inAppAvailable},${pushAvailable},${reachable}`;
  });

  // Write the updated data back to the CSV
  fs.writeFileSync(processedFilePath, finalData.join('\n'), 'utf8');
  console.log(`Updated CSV file saved to ${processedFilePath}`);
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
        resolve([]); // Return empty array if folder doesn't exist
        return;
      }
      resolve(files.filter(file => file.endsWith('.txt')));
    });
  });
}

// Iterate through all countries in countries.js and process valid users for each
async function processAllCountries() {
  const countryCodes = Object.keys(countries);

  for (const countryCode of countryCodes) {
    console.log(`Processing valid users for country: ${countryCode}`);
    await processValidUsers(countryCode);
  }
}

// Execute the process for all countries
processAllCountries().catch(err => {
  console.error('Error during processing:', err);
});
