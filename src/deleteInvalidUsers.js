const fs = require('fs');
const path = require('path');
const axios = require('axios');
const countries = require('./countries'); // Import country list

const clusters = {
    "Europe": "https://rest.fra-02.braze.eu/users/delete",
    "Global": "https://rest.iad-03.braze.com/users/delete"
};

const exportFolder = './exports'; // Parent folder where country data is stored
const auditFolder = './auditLogs'; // Folder for audit logs
const batchSize = 50;

// Ensure auditLogs directory exists
if (!fs.existsSync(auditFolder)) {
    fs.mkdirSync(auditFolder, { recursive: true });
}

async function writeAuditLog(countryCode, brazeIdsBatch) {
    const auditFile = path.join(auditFolder, `audit_${countryCode}.log`);
    const timestamp = new Date().toISOString();
    const logEntry = `${timestamp} - Deleted ${brazeIdsBatch.length} users: ${brazeIdsBatch.join(', ')}\n`;

    fs.appendFileSync(auditFile, logEntry, 'utf8');
}

async function deleteUsers(brazeIdsBatch, countryCode, clusterUrl, apiKey) {
    try {
        /*
        const response = await axios.post(
            clusterUrl,
            { braze_ids: brazeIdsBatch },
            {
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: `Bearer ${apiKey}`,
                },
            }
        );
        console.log(`[${countryCode}] Deleted ${brazeIdsBatch.length} users:`, response.data);
        */
        console.log(`[${countryCode}] Deleting ${brazeIdsBatch.length} users...`);
        await writeAuditLog(countryCode, brazeIdsBatch);
    } catch (error) {
        console.error(`[${countryCode}] Error deleting users:`, error.response ? error.response.data : error.message);
    }
}

async function processInvalidUsers() {
    for (const countryCode of Object.keys(countries)) {
        const countryConfig = countries[countryCode];
        if (!countryConfig) {
            console.error(`Missing config for country: ${countryCode}`);
            continue;
        }

        const invalidUsersFile = path.join(exportFolder, countryCode, `${countryCode}_invalid_users.txt`);

        if (!fs.existsSync(invalidUsersFile)) {
            console.log(`Skipping ${countryCode}: No invalid users file found.`);
            continue;
        }

        console.log(`Processing invalid users for ${countryCode}...`);

        const lines = fs.readFileSync(invalidUsersFile, 'utf8')
            .split('\n')
            .filter(line => line.trim() !== '');

        let brazeIds = [];

        lines.forEach(line => {
            try {
                const jsonData = JSON.parse(line);
                if (jsonData.braze_id) {
                    brazeIds.push(jsonData.braze_id);
                }
            } catch (err) {
                console.error(`[${countryCode}] Error parsing JSON:`, err);
            }
        });

        if (brazeIds.length === 0) {
            console.log(`[${countryCode}] No valid braze_ids found in ${invalidUsersFile}.`);
            continue;
        }

        console.log(`[${countryCode}] Found ${brazeIds.length} invalid users. Deleting in batches of ${batchSize}...`);

        const clusterUrl = clusters[countryConfig.cluster];
        const apiKey = countryConfig.apiKey;

        if (!clusterUrl || !apiKey) {
            console.error(`[${countryCode}] Missing cluster URL or API key.`);
            continue;
        }

        for (let i = 0; i < brazeIds.length; i += batchSize) {
            const batch = brazeIds.slice(i, i + batchSize);
            await deleteUsers(batch, countryCode, clusterUrl, apiKey);
        }

        console.log(`[${countryCode}] Finished processing all invalid users.`);
    }
}

// Run the script
processInvalidUsers();