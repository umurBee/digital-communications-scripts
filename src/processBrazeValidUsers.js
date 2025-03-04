const fs = require('fs');
const path = require('path');
const countries = require('./countries');  // Import countries configuration
const readline = require('readline');  // Use readline to stream the files line by line

const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

// Ensure the base export folder exists
const baseExportFolder = path.join(__dirname, '..', 'exports');
ensureFolderExists(baseExportFolder);

// Process the validUsers folder and create a CSV template with external_id
async function processValidUsers(countryCode) {
  // Dynamically define the folder paths for each country
  const rawFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-raw');
  const processedFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-processed'); // Remove country code folder

  // Ensure the processed folder exists
  ensureFolderExists(processedFolderPath);

  const validUsersFolderPath = path.join(rawFolderPath, 'validUsers');
  const processedFilePath = path.join(processedFolderPath, `braze-valid-users.csv`); // Save directly under braze-processed

  const files = await getTxtFiles(validUsersFolderPath);
  if (files.length === 0) {
    console.log(`No .txt files found in ${countryCode}/validUsers folder.`);
    return;
  }

  let externalIds = [];

  // Stream and process files line by line
  for (const file of files) {
    const filePath = path.join(validUsersFolderPath, file);
    const ids = await streamFileAndExtractExternalIds(filePath);
    externalIds = [...externalIds, ...ids];
  }

  // Prepare the CSV content with header
  const header = 'external_id,emailAvailable,phoneAvailable,inAppAvailable,pushAvailable\n';
  const rows = externalIds.map(id => `${id},FALSE,FALSE,FALSE,FALSE`).join('\n');
  const csvContent = header + rows;

  // Write the initial CSV with only the valid external_ids and default FALSE values
  fs.writeFileSync(processedFilePath, csvContent, 'utf8');
  console.log(`Processed CSV file saved to ${processedFilePath}`);

  // After the initial file, update it with TRUE/FALSE values based on the other folders
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
async function updateUserDataWithAvailability(externalIds, countryCode, processedFilePath) {
  const foldersToCheck = ['emailAvailableUsers', 'phoneAvailableUsers', 'inAppAvailableUsers', 'pushAvailableUsers'];

  // Read the existing CSV file data
  let finalData = fs.readFileSync(processedFilePath, 'utf8').split('\n');

  // Use Promise.all() to process folders in parallel for better performance
  const folderPromises = foldersToCheck.map(async (folder) => {
    const rawFolderPath = path.join(baseExportFolder, formattedDate, countryCode, 'braze-raw');
    const folderPath = path.join(rawFolderPath, folder);
    console.log(`Checking folder: ${folderPath}`);

    const files = await getTxtFiles(folderPath);
    const availableIds = new Set();

    // Collect all external_ids in the folder by streaming files
    const fileReads = files.map(async (file) => {
      const filePath = path.join(folderPath, file);
      const ids = await streamFileAndExtractExternalIds(filePath);
      ids.forEach(id => availableIds.add(id));
    });

    // Wait for all file reads to finish
    await Promise.all(fileReads);

    console.log(`Found ${availableIds.size} IDs in ${folder}`);

    // Update the finalData rows for each external_id
    finalData = finalData.map((line, index) => {
      const [external_id, emailAvailable, phoneAvailable, inAppAvailable, pushAvailable] = line.split(',');
      if (externalIds.includes(external_id)) {
        if (availableIds.has(external_id)) {
          // Set the appropriate column to TRUE
          const updatedRow = {
            external_id,
            emailAvailable: folder === 'emailAvailableUsers' ? 'TRUE' : emailAvailable,
            phoneAvailable: folder === 'phoneAvailableUsers' ? 'TRUE' : phoneAvailable,
            inAppAvailable: folder === 'inAppAvailableUsers' ? 'TRUE' : inAppAvailable,
            pushAvailable: folder === 'pushAvailableUsers' ? 'TRUE' : pushAvailable,
          };

          return `${updatedRow.external_id},${updatedRow.emailAvailable},${updatedRow.phoneAvailable},${updatedRow.inAppAvailable},${updatedRow.pushAvailable}`;
        }
      }
      return line; // No change if the external_id isn't found in this folder
    });
  });

  // Wait for all folder checks to finish
  await Promise.all(folderPromises);

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
        reject(err);
        return;
      }
      const txtFiles = files.filter(file => file.endsWith('.txt'));
      resolve(txtFiles);
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
