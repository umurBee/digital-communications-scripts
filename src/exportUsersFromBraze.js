const fs = require('fs');
const path = require('path');
const axios = require('axios');
const AdmZip = require('adm-zip');

const currentDate = new Date();
const formattedDate = currentDate.toISOString().split('T')[0];

const countries = require('./countries'); // Import countries object

// Define clusters and their corresponding API URLs
const clusters = {
    "Europe": "https://rest.fra-02.braze.eu/users/export/segment",
    "Global": "https://rest.iad-03.braze.com/users/export/segment"
};

// Ensure the export folder exists
const exportFolder = './exports'; // Root folder for all exports (will use date and country structure)

// Ensure the export folder and its parent folders exist
if (!fs.existsSync(exportFolder)) {
    fs.mkdirSync(exportFolder, { recursive: true });
}

// Function to trigger Braze export
async function triggerBrazeExport(apiKey, segmentId, clusterUrl, segmentName) {
    try {
        // Conditionally set fields based on segment name
        const fieldsToExport = segmentName === 'invalidUsers' ? ["braze_id"] : ["external_id"];
        
        const response = await axios.post(
            clusterUrl,
            {
                segment_id: segmentId,
                fields_to_export: fieldsToExport
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': `Bearer ${apiKey}`
                }
            }
        );

        return response.data;
    } catch (error) {
        console.error('Error triggering Braze export:', error.response?.data || error.message);
        return null;
    }
}

// Function to check if the file is available
async function isFileReady(fileUrl) {
    try {
        await axios.head(fileUrl);
        return true;
    } catch (error) {
        return false;
    }
}

// Function to wait for file readiness
async function waitForFile(url, maxAttempts = 30, interval = 5000) {
    console.log(`Waiting for Braze export to be ready...`);
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        if (await isFileReady(url)) {
            console.log(`File is ready!`);
            return true;
        }
        console.log(`Attempt ${attempt}/${maxAttempts}: File not ready yet...`);
        await new Promise(resolve => setTimeout(resolve, interval));
    }
    console.error('Max attempts reached. File is not ready.');
    return false;
}

// Function to download file
async function downloadFile(url, outputPath) {
    try {
        const response = await axios({
            method: 'GET',
            url,
            responseType: 'stream'
        });
        const writer = fs.createWriteStream(outputPath);
        response.data.pipe(writer);
        return new Promise((resolve, reject) => {
            writer.on('finish', resolve);
            writer.on('error', reject);
        });
    } catch (error) {
        console.error(`Error downloading file: ${error.message}`);
        return false;
    }
}

// Function to extract ZIP file
function extractZip(zipFilePath, extractTo) {
    try {
        const zip = new AdmZip(zipFilePath);
        zip.extractAllTo(extractTo, true);
        console.log(`Extracted ${zipFilePath} to ${extractTo}`);
    } catch (error) {
        console.error(`Error extracting ZIP: ${error.message}`);
    }
}

// Main function to export users for each segment
async function exportUsersForCountry(countryCode, countryConfig, segmentName, segmentId) {
    console.log(`\n[${formattedDate}] Starting export for ${countryCode} (${segmentName})...`);

    // Determine cluster URL
    const clusterUrl = clusters[countryConfig.cluster];
    if (!clusterUrl) {
        console.error(`Invalid cluster for ${countryCode}: ${countryConfig.cluster}`);
        return;
    }

    // 1. Trigger Braze Export
    const exportData = await triggerBrazeExport(countryConfig.apiKey, segmentId, clusterUrl, segmentName);
    if (!exportData || !exportData.url) return;

    console.log(`Braze export URL for ${countryCode} (${segmentName}): ${exportData.url}`);

    const zipUrl = exportData.url;
    const countryFolderPath = path.join(exportFolder, formattedDate);  // Using the date as the first level
    const countrySpecificFolderPath = path.join(countryFolderPath, countryCode);  // Adding the country under the date
    const brazeRawFolderPath = path.join(countrySpecificFolderPath, 'braze-raw'); // New braze-raw folder under country code

    // Ensure the date, country, and braze-raw folder structure exists
    if (!fs.existsSync(countryFolderPath)) {
        fs.mkdirSync(countryFolderPath);
    }
    if (!fs.existsSync(countrySpecificFolderPath)) {
        fs.mkdirSync(countrySpecificFolderPath);
    }
    if (!fs.existsSync(brazeRawFolderPath)) {
        fs.mkdirSync(brazeRawFolderPath);
    }

    const zipFilePath = path.join(brazeRawFolderPath, `${countryCode}.zip`);
    const extractPath = path.join(brazeRawFolderPath, `${segmentName}`);

    // 2. Wait for ZIP file to be ready
    const isReady = await waitForFile(zipUrl);
    if (!isReady) return;

    // 3. Download ZIP file
    console.log(`Downloading ZIP for ${countryCode} (${segmentName})...`);
    await downloadFile(zipUrl, zipFilePath);

    // 4. Extract ZIP file
    extractZip(zipFilePath, extractPath);

    fs.unlinkSync(zipFilePath);
    console.log(`Deleted ZIP file: ${zipFilePath}`);
}

// Main function to run exports for each country and segment
async function exportUsers() {
    for (const [countryCode, countryConfig] of Object.entries(countries)) {
        // Process each segment for the country
        for (const [segmentName, segmentId] of Object.entries(countryConfig)) {
            if (segmentName !== 'cluster' && segmentName !== 'apiKey') {
                // Create the correct path under the date and country folder
                const countryFolderPath = path.join(exportFolder, formattedDate, countryCode);  // Under the date and country
                const brazeRawFolder = path.join(countryFolderPath, 'braze-raw');  // Under the braze-raw folder

                // Ensure the country and braze-raw folder exist
                if (!fs.existsSync(countryFolderPath)) {
                    fs.mkdirSync(countryFolderPath, { recursive: true });
                }
                if (!fs.existsSync(brazeRawFolder)) {
                    fs.mkdirSync(brazeRawFolder);
                }

                // Call the export function
                await exportUsersForCountry(countryCode, countryConfig, segmentName, segmentId);
            }
        }
    }
}


// Run the script
exportUsers();