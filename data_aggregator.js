/**
 * DATA AGGREGATOR AGENT
 * =====================
 * Collects data from multiple Google Sheets and consolidates into one master sheet
 * - Fetches rows where App Link (Column C) is NOT "NOT_FOUND"
 * - Cleans and deduplicates data
 * - Appends new rows to master sheet
 * - Runs twice daily via GitHub Actions
 * 
 * Master Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL
 *   Column C: App Link
 *   Column D: App Name
 *   Column E: Video ID
 *   Column F: Source Sheet (for tracking)
 *   Column G: Date Added
 */

const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const CREDENTIALS_PATH = './credentials.json';

// Master sheet where aggregated data will be stored
const MASTER_SHEET_ID = '1yq2UwI94lwfYPY86CFwGbBsm3kpdqKrefYgrw3lEAwk';
const MASTER_SHEET_NAME = 'Text Ads data'; // Your existing tab with header

// Load source sheets configuration
const SOURCE_SHEETS_CONFIG = './source_sheets.json';

// Batch settings - Optimized for large datasets (500K+ rows)
const READ_BATCH_SIZE = 50000;  // Rows to read per batch from source sheets
const WRITE_BATCH_SIZE = 5000;  // Rows to write per batch to master sheet

// ============================================
// HELPER FUNCTIONS
// ============================================
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function loadSourceSheets() {
    try {
        const config = JSON.parse(fs.readFileSync(SOURCE_SHEETS_CONFIG, 'utf8'));
        return config.sheets || [];
    } catch (error) {
        console.error(`❌ Error loading source sheets config: ${error.message}`);
        console.log('📝 Please create source_sheets.json with your sheet IDs');
        return [];
    }
}

function cleanValue(value) {
    if (!value) return '';
    let cleaned = String(value).trim();
    
    // Remove invisible unicode characters
    cleaned = cleaned.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '');
    
    // Normalize whitespace
    cleaned = cleaned.replace(/\s+/g, ' ').trim();
    
    return cleaned;
}

function needsProcessing(appLink) {
    // Returns TRUE if App Link is NOT_FOUND (needs to be processed by your agent)
    if (!appLink) return true; // Empty = needs processing
    
    const link = appLink.trim().toUpperCase();
    
    // Skip ERROR rows - they have issues, don't collect them
    if (link === 'ERROR' || link === 'BLOCKED' || link === 'SKIP') return false;
    
    // These values mean the row needs processing
    const needsWork = ['NOT_FOUND', 'NOT FOUND', ''];
    if (needsWork.includes(link)) return true;
    
    // If it has a valid URL, it's already processed - skip it
    const lowerLink = appLink.toLowerCase();
    const hasValidLink = [
        'play.google.com',
        'itunes.apple.com',
        'apps.apple.com',
        'http',
        'https'
    ].some(pattern => lowerLink.includes(pattern));
    
    // Return true only if NO valid link (needs processing)
    return !hasValidLink;
}

function isValidRow(row) {
    // Skip rows where advertiser name or ads URL is ERROR/empty
    const advertiserName = (row[0] || '').trim().toUpperCase();
    const adsUrl = (row[1] || '').trim().toUpperCase();
    
    if (advertiserName === 'ERROR' || advertiserName === '') return false;
    if (adsUrl === 'ERROR' || adsUrl === '') return false;
    
    return true;
}

function createRowKey(row) {
    // Create unique key from Ads URL (Column B) to detect duplicates
    const adsUrl = cleanValue(row[1] || '');
    return adsUrl.toLowerCase();
}

function getPakistanTime() {
    return new Date().toLocaleString('en-PK', { 
        timeZone: 'Asia/Karachi',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit'
    });
}

// ============================================
// GOOGLE SHEETS CLIENT
// ============================================
async function getGoogleSheetsClient() {
    const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const authClient = await auth.getClient();
    return google.sheets({ version: 'v4', auth: authClient });
}

// ============================================
// READ FROM SOURCE SHEETS (supports multiple tabs)
// ============================================
async function fetchFromSourceSheet(sheets, sheetConfig) {
    const { id, name, sheetName, sheetNames } = sheetConfig;
    
    // Support both single tab (sheetName) and multiple tabs (sheetNames - can be array or string)
    let tabsToProcess;
    if (sheetNames) {
        // Handle sheetNames as either array or single string
        tabsToProcess = Array.isArray(sheetNames) ? sheetNames : [sheetNames];
    } else {
        tabsToProcess = [sheetName || 'Sheet1'];
    }
    
    const allValidRows = [];
    
    console.log(`\n📖 Reading from: ${name} (${id})`);
    console.log(`   Tabs to process: ${tabsToProcess.join(', ')}`);
    
    for (const tabName of tabsToProcess) {
        const validRows = await fetchFromSingleTab(sheets, id, name, tabName);
        allValidRows.push(...validRows);
        
        // Small delay between tabs
        if (tabsToProcess.length > 1) {
            await sleep(200);
        }
    }
    
    console.log(`   ✅ Total from ${name}: ${allValidRows.length} valid rows from ${tabsToProcess.length} tab(s)`);
    return allValidRows;
}

async function fetchFromSingleTab(sheets, spreadsheetId, sourceName, tabName) {
    // Escape tab names with spaces, parentheses, or other special characters
    const needsQuotes = /[\s\(\)\-\'\"\!\@\#\$\%\^\&\*]/.test(tabName);
    const escapedTabName = needsQuotes ? `'${tabName}'` : tabName;
    const validRows = [];
    
    console.log(`   📑 Reading tab: ${tabName}`);
    
    try {
        // Get total rows using metadata
        let totalRows = 0;
        try {
            const metadata = await sheets.spreadsheets.get({
                spreadsheetId: spreadsheetId,
                ranges: [escapedTabName],
                fields: 'sheets.properties.gridProperties.rowCount'
            });
            totalRows = metadata.data.sheets?.[0]?.properties?.gridProperties?.rowCount || 0;
            console.log(`      Rows in tab: ${totalRows}`);
        } catch (metaError) {
            console.log(`      ⚠️ Could not get metadata for ${tabName}, using default batch size`);
            totalRows = 100000; // Fallback
        }
        
        if (totalRows <= 1) {
            console.log(`      ⚠️ No data rows found in ${tabName}`);
            return validRows;
        }
        
        // Read in batches
        let startRow = 2; // Skip header
        let processedRows = 0;
        
        while (startRow <= totalRows) {
            const endRow = Math.min(startRow + READ_BATCH_SIZE - 1, totalRows);
            const range = `${escapedTabName}!A${startRow}:E${endRow}`;
            
            try {
                const response = await sheets.spreadsheets.values.get({
                    spreadsheetId: spreadsheetId,
                    range: range,
                });
                
                const rows = response.data.values || [];
                
                if (rows.length === 0) break;
                
                // Filter rows where App Link is NOT_FOUND (needs processing)
                // Also skip rows with ERROR values
                for (const row of rows) {
                    const appLink = row[2] || ''; // Column C
                    
                    // Skip invalid rows (ERROR, empty advertiser/URL)
                    if (!isValidRow(row)) continue;
                    
                    if (needsProcessing(appLink)) {
                        validRows.push({
                            advertiserName: cleanValue(row[0] || ''),
                            adsUrl: cleanValue(row[1] || ''),
                            appLink: cleanValue(row[2] || 'NOT_FOUND'),
                            appName: cleanValue(row[3] || ''),
                            videoId: cleanValue(row[4] || '')
                        });
                    }
                }
                
                processedRows += rows.length;
                startRow = endRow + 1;
                
                // Small delay to avoid rate limits
                await sleep(100);
                
            } catch (batchError) {
                console.log(`      ⚠️ Error reading batch ${startRow}-${endRow}: ${batchError.message}`);
                startRow += READ_BATCH_SIZE;
                await sleep(500);
            }
        }
        
        console.log(`      ✅ Found ${validRows.length} valid rows in ${tabName}`);
        
    } catch (error) {
        console.error(`      ❌ Error reading tab ${tabName}: ${error.message}`);
    }
    
    return validRows;
}

// ============================================
// GET EXISTING DATA FROM MASTER SHEET
// ============================================
async function getExistingKeys(sheets) {
    const existingKeys = new Set();
    const escapedSheetName = MASTER_SHEET_NAME.includes(' ') ? `'${MASTER_SHEET_NAME}'` : MASTER_SHEET_NAME;
    
    console.log(`\n📋 Loading existing data from master sheet...`);
    
    try {
        // Check if sheet exists, create if not
        const sheetMetadata = await sheets.spreadsheets.get({
            spreadsheetId: MASTER_SHEET_ID,
            fields: 'sheets.properties.title'
        });
        
        const sheetExists = sheetMetadata.data.sheets?.some(
            s => s.properties?.title === MASTER_SHEET_NAME
        );
        
        if (!sheetExists) {
            console.log(`   Creating new sheet: ${MASTER_SHEET_NAME}`);
            await sheets.spreadsheets.batchUpdate({
                spreadsheetId: MASTER_SHEET_ID,
                resource: {
                    requests: [{
                        addSheet: {
                            properties: { title: MASTER_SHEET_NAME }
                        }
                    }]
                }
            });
            
            // Add header row
            await sheets.spreadsheets.values.update({
                spreadsheetId: MASTER_SHEET_ID,
                range: `${escapedSheetName}!A1:G1`,
                valueInputOption: 'RAW',
                resource: {
                    values: [['Advertiser Name', 'Ads URL', 'App Link', 'App Name', 'Video ID', 'Source Sheet', 'Date Added']]
                }
            });
            
            console.log(`   ✅ Created sheet with headers`);
            return existingKeys;
        }
        
        // Get existing Ads URLs (Column B) to detect duplicates
        let totalRows = 0;
        try {
            const metadata = await sheets.spreadsheets.get({
                spreadsheetId: MASTER_SHEET_ID,
                ranges: [escapedSheetName],
                fields: 'sheets.properties.gridProperties.rowCount'
            });
            totalRows = metadata.data.sheets?.[0]?.properties?.gridProperties?.rowCount || 0;
        } catch (e) {
            totalRows = 100000;
        }
        
        if (totalRows <= 1) {
            // Only header exists, add it if missing
            try {
                await sheets.spreadsheets.values.update({
                    spreadsheetId: MASTER_SHEET_ID,
                    range: `${escapedSheetName}!A1:G1`,
                    valueInputOption: 'RAW',
                    resource: {
                        values: [['Advertiser Name', 'Ads URL', 'App Link', 'App Name', 'Video ID', 'Source Sheet', 'Date Added']]
                    }
                });
            } catch (e) { /* Header might exist */ }
            return existingKeys;
        }
        
        // Read existing URLs in batches
        let startRow = 2;
        while (startRow <= totalRows) {
            const endRow = Math.min(startRow + READ_BATCH_SIZE - 1, totalRows);
            
            try {
                const response = await sheets.spreadsheets.values.get({
                    spreadsheetId: MASTER_SHEET_ID,
                    range: `${escapedSheetName}!B${startRow}:B${endRow}`,
                });
                
                const rows = response.data.values || [];
                if (rows.length === 0) break;
                
                for (const row of rows) {
                    if (row[0]) {
                        existingKeys.add(row[0].toLowerCase().trim());
                    }
                }
                
                startRow = endRow + 1;
                await sleep(100);
                
            } catch (e) {
                startRow += READ_BATCH_SIZE;
            }
        }
        
        console.log(`   ✅ Found ${existingKeys.size} existing entries`);
        
    } catch (error) {
        console.error(`   ❌ Error reading master sheet: ${error.message}`);
    }
    
    return existingKeys;
}

// ============================================
// CREATE NEW TAB WITH HEADER
// ============================================
async function createNewTab(sheets, tabName) {
    const escapedTabName = tabName.includes(' ') ? `'${tabName}'` : tabName;
    
    try {
        // Create the new tab
        await sheets.spreadsheets.batchUpdate({
            spreadsheetId: MASTER_SHEET_ID,
            resource: {
                requests: [{
                    addSheet: {
                        properties: { title: tabName }
                    }
                }]
            }
        });
        
        // Add header row
        await sheets.spreadsheets.values.update({
            spreadsheetId: MASTER_SHEET_ID,
            range: `${escapedTabName}!A1:E1`,
            valueInputOption: 'RAW',
            resource: {
                values: [['Advitiser Name', 'Ads URL', 'App Link', 'App Name', 'Video ID']]
            }
        });
        
        console.log(`   ✅ Created new tab: ${tabName}`);
        return true;
    } catch (error) {
        console.error(`   ❌ Error creating tab ${tabName}: ${error.message}`);
        return false;
    }
}

// ============================================
// WRITE TO MASTER SHEET (SINGLE TAB - ALL DATA)
// ============================================
async function writeToMasterSheet(sheets, newRows) {
    if (newRows.length === 0) {
        console.log(`\n📝 No new rows to write`);
        return;
    }
    
    const escapedSheetName = MASTER_SHEET_NAME.includes(' ') ? `'${MASTER_SHEET_NAME}'` : MASTER_SHEET_NAME;
    
    console.log(`\n📝 Writing ${newRows.length} new rows to master sheet...`);
    console.log(`   ⚠️ Header row (Row 1) will NOT be modified`);
    console.log(`   📊 Writing ALL data to single tab: ${MASTER_SHEET_NAME}`);
    
    // Convert to array format for sheets API - ONLY 3 COLUMNS: A, B, C
    const values = newRows.map(row => [
        row.advertiserName,
        row.adsUrl,
        row.appLink
    ]);
    
    try {
        // Write in batches
        for (let i = 0; i < values.length; i += WRITE_BATCH_SIZE) {
            const batch = values.slice(i, i + WRITE_BATCH_SIZE);
            
            await sheets.spreadsheets.values.append({
                spreadsheetId: MASTER_SHEET_ID,
                range: `${escapedSheetName}!A2:C`,
                valueInputOption: 'RAW',
                insertDataOption: 'INSERT_ROWS',
                resource: { values: batch }
            });
            
            const batchEnd = Math.min(i + WRITE_BATCH_SIZE, values.length);
            console.log(`   ✅ Wrote batch ${i + 1} - ${batchEnd}`);
            
            if (i + WRITE_BATCH_SIZE < values.length) {
                await sleep(500);
            }
        }
        
        console.log(`\n   ✅ Successfully wrote all ${newRows.length} rows to ${MASTER_SHEET_NAME}`);
        
    } catch (error) {
        console.error(`   ❌ Error writing to master sheet: ${error.message}`);
        throw error;
    }
}

// ============================================
// MAIN EXECUTION
// ============================================
async function main() {
    console.log('═══════════════════════════════════════════════════════════');
    console.log('🔄 DATA AGGREGATOR AGENT');
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`📅 Run Time: ${getPakistanTime()}`);
    console.log(`📋 Master Sheet: ${MASTER_SHEET_ID}`);
    
    // Load source sheets configuration
    const sourceSheets = loadSourceSheets();
    
    if (sourceSheets.length === 0) {
        console.error('\n❌ No source sheets configured. Please update source_sheets.json');
        process.exit(1);
    }
    
    console.log(`\n📚 Source Sheets: ${sourceSheets.length}`);
    sourceSheets.forEach((sheet, i) => {
        console.log(`   ${i + 1}. ${sheet.name} (${sheet.id})`);
    });
    
    // Initialize Google Sheets client
    const sheets = await getGoogleSheetsClient();
    
    // Get existing entries to avoid duplicates
    const existingKeys = await getExistingKeys(sheets);
    
    // Collect data from all source sheets
    const allValidRows = [];
    const stats = {
        totalProcessed: 0,
        totalValid: 0,
        totalNew: 0,
        bySheet: {}
    };
    
    for (const sheetConfig of sourceSheets) {
        const validRows = await fetchFromSourceSheet(sheets, sheetConfig);
        
        stats.bySheet[sheetConfig.name] = {
            valid: validRows.length,
            new: 0
        };
        
        // Filter out duplicates
        for (const row of validRows) {
            const key = row.adsUrl.toLowerCase();
            
            if (!existingKeys.has(key)) {
                allValidRows.push(row);
                existingKeys.add(key); // Prevent duplicates within same run
                stats.bySheet[sheetConfig.name].new++;
            }
        }
        
        stats.totalValid += validRows.length;
        
        // Delay between sheets to avoid rate limits
        await sleep(1000);
    }
    
    stats.totalNew = allValidRows.length;
    
    // Write new rows to master sheet
    await writeToMasterSheet(sheets, allValidRows);
    
    // Print summary
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('📊 SUMMARY');
    console.log('═══════════════════════════════════════════════════════════');
    console.log(`Total valid rows found: ${stats.totalValid}`);
    console.log(`New rows added: ${stats.totalNew}`);
    console.log(`Duplicates skipped: ${stats.totalValid - stats.totalNew}`);
    console.log('\nBy Source Sheet:');
    for (const [name, data] of Object.entries(stats.bySheet)) {
        console.log(`   ${name}: ${data.valid} valid, ${data.new} new`);
    }
    console.log('\n✅ Aggregation complete!');
    console.log('═══════════════════════════════════════════════════════════\n');
}

// Run the aggregator
main().catch(error => {
    console.error('❌ Fatal error:', error.message);
    process.exit(1);
});
