/**
 * APP NAME EXTRACTION AGENT (BOTTOM TO TOP)
 * =====================================
 * Extracts App Name from Google Ads Transparency URLs
 * Processes rows from BOTTOM to TOP in batches
 * 
 * Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL
 *   Column D: App Name
 *   Column M: Timestamp
 */

// EXACT IMPORTS FROM app_data_agent.js
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());
const { google } = require('googleapis');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1l4JpCcA1GSkta1CE77WxD_YCgePHI87K7NtMu1Sd4Q0';
const SHEET_NAME = process.env.SHEET_NAME || 'Test'; // Can be overridden via env var
// Escape sheet name for use in A1 notation (wrap in single quotes if it contains spaces)
const ESCAPED_SHEET_NAME = SHEET_NAME.includes(' ') ? `'${SHEET_NAME}'` : SHEET_NAME;
const CREDENTIALS_PATH = './credentials.json';
const SHEET_BATCH_SIZE = parseInt(process.env.SHEET_BATCH_SIZE) || 10000; // Rows to load per batch
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 5; // Balanced: faster but safe
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 4;
const POST_CLICK_WAIT = 6000;
const RETRY_WAIT_MULTIPLIER = 1.25;
const PAGE_LOAD_DELAY_MIN = parseInt(process.env.PAGE_LOAD_DELAY_MIN) || 1000; // Faster staggered starts
const PAGE_LOAD_DELAY_MAX = parseInt(process.env.PAGE_LOAD_DELAY_MAX) || 3000;

const BATCH_DELAY_MIN = parseInt(process.env.BATCH_DELAY_MIN) || 5000; // Balanced: faster but safe
const BATCH_DELAY_MAX = parseInt(process.env.BATCH_DELAY_MAX) || 10000; // Balanced: faster but safe

const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];
const MAX_PROXY_ATTEMPTS = parseInt(process.env.MAX_PROXY_ATTEMPTS) || Math.max(3, PROXIES.length);
const PROXY_RETRY_DELAY_MIN = parseInt(process.env.PROXY_RETRY_DELAY_MIN) || 25000;
const PROXY_RETRY_DELAY_MAX = parseInt(process.env.PROXY_RETRY_DELAY_MAX) || 75000;

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

const proxyStats = { totalBlocks: 0, perProxy: {} };

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
];

const VIEWPORTS = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768 },
    { width: 1536, height: 864 },
    { width: 1440, height: 900 },
    { width: 1280, height: 720 },
    { width: 1600, height: 900 },
    { width: 1920, height: 1200 },
    { width: 1680, height: 1050 }
];

const randomDelay = (min, max) => new Promise(r => setTimeout(r, min + Math.random() * (max - min)));
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ============================================
// GOOGLE SHEETS
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

async function getUrlData(sheets, batchSize = SHEET_BATCH_SIZE) {
    const toProcess = [];

    // First, get the total number of rows using sheet metadata (supports 40,000+ rows)
    // Then scan ALL rows from absolute bottom to top
    console.log(`📊 Finding total rows and scanning ALL data from BOTTOM to TOP in batches of ${batchSize} rows...`);

    // Get the actual total row count using sheet metadata (supports 40,000+ rows)
    let totalRows = 0;
    let metadataRowCount = 0;
    try {
        // Use spreadsheet metadata to get actual row count
        const sheetMetadata = await sheets.spreadsheets.get({
            spreadsheetId: SPREADSHEET_ID,
            ranges: [`${ESCAPED_SHEET_NAME}`],
            fields: 'sheets.properties.gridProperties.rowCount'
        });

        // Get the row count from metadata - this is our PRIMARY source of truth
        const sheetProps = sheetMetadata.data.sheets?.[0]?.properties?.gridProperties;
        if (sheetProps && sheetProps.rowCount) {
            metadataRowCount = sheetProps.rowCount;
            console.log(`  ✓ Sheet metadata indicates ${metadataRowCount} total rows`);
        }

        // Use metadata row count as primary, but try to find last row with actual data
        // by checking column B (URL column) from bottom up in chunks
        // This handles cases where metadata rowCount includes empty rows
        if (metadataRowCount > 0) {
            totalRows = metadataRowCount;

            // Try to find the actual last row with data by checking from bottom
            // Check in reverse chunks of 1000 to find where data ends
            let foundLastDataRow = false;
            let checkEnd = metadataRowCount;

            while (!foundLastDataRow && checkEnd > 1) {
                const checkStart = Math.max(2, checkEnd - 1000);
                try {
                    const checkResponse = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `${ESCAPED_SHEET_NAME}!B${checkStart}:B${checkEnd}`,
                    });
                    const checkRows = checkResponse.data.values || [];

                    // Find last non-empty row in this chunk
                    for (let i = checkRows.length - 1; i >= 0; i--) {
                        const cell = checkRows[i]?.[0]?.trim();
                        if (cell && cell.length > 0) {
                            totalRows = checkStart + i;
                            foundLastDataRow = true;
                            console.log(`  ✓ Found actual last data row: ${totalRows}`);
                            break;
                        }
                    }

                    if (!foundLastDataRow) {
                        checkEnd = checkStart - 1;
                    }
                } catch (e) {
                    // If error, assume this chunk has data and use metadata count
                    foundLastDataRow = true;
                }
            }
        }

        console.log(`  ✓ Will scan ${totalRows} rows from BOTTOM (row ${totalRows}) to TOP (row 2)`);
    } catch (error) {
        console.error(`  ⚠️ Error finding total rows: ${error.message}`);
        // Fallback: try to get rows in batches from a large assumed number
        totalRows = 100000; // Assume large number, will stop when no more data
    }

    if (totalRows <= 1) {
        console.log(`📊 No data rows found\n`);
        return toProcess;
    }

    // Process from bottom to top in batches
    let endRow = totalRows; // Start from the last row
    let hasMoreData = true;
    let totalProcessed = 0;

    while (hasMoreData && endRow > 1) {
        try {
            // Calculate start row for this batch (working backwards)
            const startRow = Math.max(2, endRow - batchSize + 1); // Row 2 is first data row (skip header)
            const range = `${ESCAPED_SHEET_NAME}!A${startRow}:E${endRow}`;

            const response = await sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: range,
            });

            const rows = response.data.values || [];

            if (rows.length === 0) {
                hasMoreData = false;
                break;
            }

            // Process rows in reverse order (from bottom to top within this batch)
            for (let i = rows.length - 1; i >= 0; i--) {
                const row = rows[i];
                const actualRowIndex = startRow + i - 1; // Actual row number in sheet (0-indexed from startRow)
                const url = row[1]?.trim() || '';
                const appName = row[3]?.trim() || '';

                // Skip if no URL
                if (!url) continue;

                // Skip rows that already have App Name
                if (appName && appName !== 'NOT_FOUND') {
                    continue;
                }

                // Row needs App Name extraction
                toProcess.push({
                    url,
                    rowIndex: actualRowIndex
                });
            }

            totalProcessed += rows.length;
            console.log(`  ✓ Processed ${totalProcessed} rows (from bottom), found ${toProcess.length} to process`);

            // Move to next batch (going backwards)
            endRow = startRow - 1;

            // If we've reached row 1 (header), we're done
            if (endRow <= 1) {
                hasMoreData = false;
            } else {
                // Small delay between batches to avoid rate limits
                await sleep(100);
            }
        } catch (error) {
            console.error(`  ⚠️ Error loading batch ending at row ${endRow}: ${error.message}`);
            // If error, try to continue with next batch (move backwards)
            endRow -= batchSize;
            if (endRow <= 1) {
                hasMoreData = false;
            }
            await sleep(500); // Wait a bit longer on error
        }
    }

    console.log(`📊 Total: ${totalProcessed} rows scanned, ${toProcess.length} need processing (from bottom to top)\n`);
    return toProcess;
}

async function batchWriteToSheet(sheets, updates, retryCount = 0) {
    if (updates.length === 0) return;

    const MAX_WRITE_RETRIES = 5;
    const BASE_RETRY_DELAY = 5000; // 5 seconds base delay

    const data = [];
    updates.forEach(({ rowIndex, appName }) => {
        const rowNum = rowIndex + 1;

        // Write app name (always write something)
        const appNameValue = appName || 'NOT_FOUND';
        data.push({ range: `${ESCAPED_SHEET_NAME}!D${rowNum}`, values: [[appNameValue]] });

        // Write Timestamp to Column M (Pakistan Time)
        const timestamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
        data.push({ range: `${ESCAPED_SHEET_NAME}!M${rowNum}`, values: [[timestamp]] });
    });

    if (data.length === 0) return;

    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: { valueInputOption: 'RAW', data: data }
        });
        console.log(`  ✅ Wrote ${updates.length} results to sheet`);
    } catch (error) {
        const errorMessage = error.message || '';
        const isRateLimit = errorMessage.includes('429') || errorMessage.includes('Quota') ||
            errorMessage.includes('rate') || errorMessage.includes('RATE_LIMIT');
        const isTransient = errorMessage.includes('503') || errorMessage.includes('500') ||
            errorMessage.includes('UNAVAILABLE') || errorMessage.includes('timeout');

        if ((isRateLimit || isTransient) && retryCount < MAX_WRITE_RETRIES) {
            // Exponential backoff: 5s, 10s, 20s, 40s, 80s
            const retryDelay = BASE_RETRY_DELAY * Math.pow(2, retryCount);
            console.log(`  ⚠️ Sheet write error (${errorMessage.substring(0, 50)}...). Retry ${retryCount + 1}/${MAX_WRITE_RETRIES} in ${retryDelay / 1000}s...`);
            await sleep(retryDelay);
            return batchWriteToSheet(sheets, updates, retryCount + 1);
        } else if (retryCount < MAX_WRITE_RETRIES) {
            // Non-rate-limit error, still retry with shorter delay
            const retryDelay = 3000;
            console.log(`  ⚠️ Sheet write error: ${errorMessage}. Retry ${retryCount + 1}/${MAX_WRITE_RETRIES} in ${retryDelay / 1000}s...`);
            await sleep(retryDelay);
            return batchWriteToSheet(sheets, updates, retryCount + 1);
        } else {
            console.error(`  ❌ Sheet write FAILED after ${MAX_WRITE_RETRIES} retries: ${errorMessage}`);
            // Log which rows failed so they can be identified
            const failedRows = updates.map(u => u.rowIndex + 1).join(', ');
            console.error(`  ❌ Failed rows: ${failedRows}`);
        }
    }
}

// ============================================
// APP NAME EXTRACTION - ONE VISIT PER URL
// ============================================
async function extractAppName(url, browser, attempt = 1) {
    const page = await browser.newPage();
    let result = {
        appName: 'NOT_FOUND'
    };

    // Clean name function - removes CSS garbage and normalizes
    const cleanName = (name) => {
        if (!name) return 'NOT_FOUND';
        let cleaned = name.trim();

        // Remove invisible unicode
        cleaned = cleaned.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '');

        // Remove CSS-like patterns
        cleaned = cleaned.replace(/[a-zA-Z-]+\s*:\s*[^;]+;?/g, ' ');
        cleaned = cleaned.replace(/\d+px/g, ' ');
        cleaned = cleaned.replace(/\*+/g, ' ');
        cleaned = cleaned.replace(/\.[a-zA-Z][\w-]*/g, ' ');

        // Remove special markers
        cleaned = cleaned.split('!@~!@~')[0];
        if (cleaned.includes('|')) {
            cleaned = cleaned.split('|')[0];
        }

        // Normalize whitespace
        cleaned = cleaned.replace(/\s+/g, ' ').trim();

        // Length check
        if (cleaned.length < 2 || cleaned.length > 80) return 'NOT_FOUND';

        // Reject if looks like CSS
        if (/:\s*\d/.test(cleaned) || cleaned.includes('height') || cleaned.includes('width') || cleaned.includes('font')) {
            return 'NOT_FOUND';
        }

        return cleaned || 'NOT_FOUND';
    };

    // ENHANCED ANTI-DETECTION - More comprehensive fingerprint masking
    const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    await page.setUserAgent(userAgent);

    const viewport = VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)];
    await page.setViewport(viewport);

    // Random screen properties for more realistic fingerprint
    const screenWidth = viewport.width + Math.floor(Math.random() * 100) - 50;
    const screenHeight = viewport.height + Math.floor(Math.random() * 100) - 50;

    await page.evaluateOnNewDocument((screenW, screenH) => {
        // Remove webdriver flag
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

        // Chrome runtime
        window.chrome = { runtime: {} };

        // Plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
            configurable: true
        });

        // Languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
            configurable: true
        });

        // Platform
        Object.defineProperty(navigator, 'platform', {
            get: () => /Win/.test(navigator.userAgent) ? 'Win32' :
                /Mac/.test(navigator.userAgent) ? 'MacIntel' : 'Linux x86_64',
            configurable: true
        });

        // Hardware concurrency (randomize CPU cores)
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 4 + Math.floor(Math.random() * 4), // 4-8 cores
            configurable: true
        });

        // Device memory (randomize RAM)
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => [4, 8, 16][Math.floor(Math.random() * 3)],
            configurable: true
        });

        // Screen properties
        Object.defineProperty(screen, 'width', { get: () => screenW, configurable: true });
        Object.defineProperty(screen, 'height', { get: () => screenH, configurable: true });
        Object.defineProperty(screen, 'availWidth', { get: () => screenW, configurable: true });
        Object.defineProperty(screen, 'availHeight', { get: () => screenH - 40, configurable: true });

        // Permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        // Canvas fingerprint protection (add noise)
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function () {
            const context = this.getContext('2d');
            if (context) {
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {
                    imageData.data[i] += Math.random() * 0.01 - 0.005; // Tiny noise
                }
                context.putImageData(imageData, 0, 0);
            }
            return originalToDataURL.apply(this, arguments);
        };
    }, screenWidth, screenHeight);

    // SPEED OPTIMIZATION - Block unnecessary resources
    await page.setRequestInterception(true);
    page.on('request', (request) => {
        const requestUrl = request.url();
        const resourceType = request.resourceType();
        const blockedTypes = ['image', 'font', 'other', 'stylesheet'];
        const blockedPatterns = [
            'analytics', 'google-analytics', 'doubleclick', 'pagead',
            'facebook.com', 'bing.com', 'logs', 'collect', 'securepubads'
        ];

        if (blockedTypes.includes(resourceType) || blockedPatterns.some(p => requestUrl.includes(p))) {
            request.abort();
        } else {
            request.continue();
        }
    });

    try {
        console.log(`  🚀 Loading (${viewport.width}x${viewport.height}): ${url.substring(0, 50)}...`);

        // Random mouse movement before page load (more human-like)
        try {
            const client = await page.target().createCDPSession();
            await client.send('Input.dispatchMouseEvent', {
                type: 'mouseMoved',
                x: Math.random() * viewport.width,
                y: Math.random() * viewport.height
            });
        } catch (e) { /* Ignore if CDP not ready */ }

        // Enhanced headers with randomization
        const acceptLanguages = [
            'en-US,en;q=0.9',
            'en-US,en;q=0.9,zh-CN;q=0.8',
            'en-US,en;q=0.9,fr;q=0.8',
            'en-GB,en;q=0.9',
            'en-US,en;q=0.9,es;q=0.8'
        ];
        await page.setExtraHTTPHeaders({
            'accept-language': acceptLanguages[Math.floor(Math.random() * acceptLanguages.length)],
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'accept-encoding': 'gzip, deflate, br',
            'sec-ch-ua': `"Not_A Brand";v="8", "Chromium";v="${120 + Math.floor(Math.random() * 2)}", "Google Chrome";v="${120 + Math.floor(Math.random() * 2)}"`,
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': `"${/Win/.test(userAgent) ? 'Windows' : /Mac/.test(userAgent) ? 'macOS' : 'Linux'}"`,
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1'
        });

        // Increased wait strategy for accuracy - iframes need time to render content
        const response = await page.goto(url, { waitUntil: 'networkidle2', timeout: MAX_WAIT_TIME });

        const content = await page.content();
        if ((response && response.status && response.status() === 429) ||
            content.includes('Our systems have detected unusual traffic') ||
            content.includes('Too Many Requests') ||
            content.toLowerCase().includes('captcha') ||
            content.toLowerCase().includes('g-recaptcha') ||
            content.toLowerCase().includes('verify you are human')) {
            console.error('  ⚠️ BLOCKED');
            await page.close();
            return { appName: 'BLOCKED' };
        }

        // Wait for dynamic elements to settle (increased for large datasets)
        const baseWait = 4000 + Math.random() * 2000; // Increased: 4000-6000ms for better iframe loading
        const attemptMultiplier = Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait * attemptMultiplier);

        // Additional wait specifically for iframes to render (critical for Play Store links in large datasets)
        try {
            await page.evaluate(async () => {
                const iframes = document.querySelectorAll('iframe');
                if (iframes.length > 0) {
                    await new Promise(resolve => {
                        let loaded = 0;
                        const totalIframes = iframes.length;
                        const checkLoaded = () => {
                            loaded++;
                            if (loaded >= totalIframes) {
                                setTimeout(resolve, 1500); // Extra time after all iframes load
                            }
                        };
                        iframes.forEach(iframe => {
                            try {
                                if (iframe.contentDocument && iframe.contentDocument.readyState === 'complete') {
                                    checkLoaded();
                                } else {
                                    iframe.onload = checkLoaded;
                                    // Timeout after 4 seconds per iframe
                                    setTimeout(checkLoaded, 4000);
                                }
                            } catch (e) {
                                // Cross-origin iframe, count as loaded
                                checkLoaded();
                            }
                        });
                        // If no iframes, resolve immediately
                        if (totalIframes === 0) resolve();
                    });
                }
            });
        } catch (e) {
            // If iframe check fails, wait a bit anyway
            await sleep(1000);
        }

        // Random mouse movements for more human-like behavior
        try {
            const client = await page.target().createCDPSession();
            const movements = 2 + Math.floor(Math.random() * 3); // 2-4 movements
            for (let i = 0; i < movements; i++) {
                await client.send('Input.dispatchMouseEvent', {
                    type: 'mouseMoved',
                    x: Math.random() * viewport.width,
                    y: Math.random() * viewport.height
                });
                await sleep(200 + Math.random() * 300);
            }
        } catch (e) { /* Ignore if CDP fails */ }

        // Human-like interaction (optimized for speed while staying safe)
        await page.evaluate(async () => {
            // Quick but natural scrolling with random pauses
            for (let i = 0; i < 3; i++) {
                window.scrollBy(0, 150 + Math.random() * 100);
                await new Promise(r => setTimeout(r, 200 + Math.random() * 150));
                // Random pause sometimes (30% chance)
                if (Math.random() < 0.3) {
                    await new Promise(r => setTimeout(r, 300 + Math.random() * 200));
                }
            }
            // Scroll back up a bit
            window.scrollBy(0, -100);
            await new Promise(r => setTimeout(r, 250));
        });

        // Random pause before extraction (10-30% chance, adds randomness)
        if (Math.random() < 0.2) {
            const randomPause = 500 + Math.random() * 1000;
            await sleep(randomPause);
        }

        // =====================================================
        // APP NAME EXTRACTION
        // =====================================================
        console.log(`  📊 Extracting App Name...`);

        const frames = page.frames();
        for (const frame of frames) {
            try {
                const frameData = await frame.evaluate(() => {
                    const data = { appName: null };
                    const root = document.querySelector('#portrait-landscape-phone') || document.body;

                    // Check if this frame content is visible (has dimensions)
                    const bodyRect = document.body.getBoundingClientRect();
                    if (bodyRect.width < 50 || bodyRect.height < 50) {
                        return { ...data, isHidden: true };
                    }

                    // =====================================================
                    // CLEAN APP NAME
                    // =====================================================
                    const cleanAppName = (text) => {
                        if (!text || typeof text !== 'string') return null;
                        let clean = text.trim();
                        clean = clean.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '');
                        clean = clean.replace(/\.[a-zA-Z][\w-]*/g, ' ');
                        clean = clean.replace(/[a-zA-Z-]+\s*:\s*[^;]+;/g, ' ');
                        clean = clean.split('!@~!@~')[0];
                        if (clean.includes('|')) {
                            const parts = clean.split('|').map(p => p.trim()).filter(p => p.length > 2);
                            if (parts.length > 0) clean = parts[0];
                        }
                        clean = clean.replace(/\s+/g, ' ').trim();
                        if (clean.length < 2 || clean.length > 80) return null;
                        if (/^[\d\s\W]+$/.test(clean)) return null;
                        
                        // Filter out blacklisted words
                        const blacklistWords = ['ad details', 'google ads', 'transparency center', 'about this ad', 'install', 'open', 'download', 'play', 'get'];
                        if (blacklistWords.some(word => clean.toLowerCase() === word || (clean.toLowerCase().includes(word) && clean.length < 15))) return null;
                        
                        return clean;
                    };

                    // =====================================================
                    // APP NAME SELECTORS (Priority Order)
                    // =====================================================
                    const appNameSelectors = [
                        'a[data-asoch-targets*="ochAppName"]',
                        'a[data-asoch-targets*="AppName"]',
                        '[data-asoch-targets*="AppName"]',
                        'a[data-asoch-targets*="appname" i]',
                        'a[data-asoch-targets*="rrappname" i]',
                        'a[class*="short-app-name"]',
                        '.short-app-name a',
                        '.app-name',
                        '[class*="app-name"]',
                        '#creative-brand-name',
                        'div[role="heading"]',
                        '[aria-label="App Name"]',
                        'h1', 'h2', 'h3',
                        '.headline',
                        '#creative-headline',
                        '[class*="title"]',
                        '[class*="brand"]',
                        '[class*="appTitle"]'
                    ];

                    for (const selector of appNameSelectors) {
                        const elements = root.querySelectorAll(selector);
                        for (const el of elements) {
                            const rawName = el.innerText || el.textContent || '';
                            const appName = cleanAppName(rawName);
                            if (appName) {
                                return { appName, isHidden: false };
                            }
                        }
                    }

                    // Fallback: scan visible text lines
                    if (!data.appName) {
                        const textLines = (root.innerText || '').split(/\n|\r/).map(x => x.trim()).filter(Boolean);
                        for (const line of textLines) {
                            const appName = cleanAppName(line);
                            if (appName && appName.length > 3 && appName.length < 50) {
                                data.appName = appName;
                                break;
                            }
                        }
                    }

                    data.isHidden = false;
                    return data;
                });

                // Skip hidden frames
                if (frameData.isHidden) continue;

                // If we found app name, use it
                if (frameData.appName && result.appName === 'NOT_FOUND') {
                    result.appName = cleanName(frameData.appName);
                    console.log(`  ✓ Found App Name: ${result.appName}`);
                    break; // We have the name, stop searching
                }
            } catch (e) { }
        }

        // Final fallback from Meta/Title
        if (result.appName === 'NOT_FOUND' || result.appName === 'Ad Details') {
            try {
                const title = await page.title();
                if (title && !title.toLowerCase().includes('google ads') && !title.toLowerCase().includes('transparency')) {
                    result.appName = cleanName(title.split(' - ')[0].split('|')[0].trim());
                }
            } catch (e) { }
        }

        await page.close();
        return result;
    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        await page.close();
        return { appName: 'ERROR' };
    }
}

async function extractWithRetry(item, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        if (attempt > 1) console.log(`  🔄 Retry ${attempt}/${MAX_RETRIES}...`);

        const data = await extractAppName(item.url, browser, attempt);

        if (data.appName === 'BLOCKED') return data;

        // Success: we found an app name
        if (data.appName && data.appName !== 'NOT_FOUND' && data.appName !== 'ERROR') {
            return data;
        }

        await randomDelay(2000, 4000);
    }
    // If we're here, we exhausted retries. Return whatever we have.
    return { appName: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting App Name Extraction Agent (BOTTOM TO TOP)...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Columns: B=URL, D=App Name\n`);

    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000;

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ All rows complete. Nothing to process.');
        process.exit(0);
    }

    console.log(`📊 Found ${toProcess.length} rows to process (from bottom to top)\n`);

    console.log(PROXIES.length ? `🔁 Proxy rotation enabled (${PROXIES.length} proxies)` : '🔁 Running direct');

    const PAGES_PER_BROWSER = 30; // Balanced: faster but safe
    let currentIndex = 0;
    let consecutiveSuccessBatches = 0;

    while (currentIndex < toProcess.length) {
        if (Date.now() - sessionStartTime > MAX_RUNTIME) {
            console.log('\n⏰ Time limit reached. Stopping.');
            process.exit(0);
        }

        const remainingCount = toProcess.length - currentIndex;
        const currentSessionSize = Math.min(PAGES_PER_BROWSER, remainingCount);

        console.log(`\n🏢 Starting New Browser Session (Items ${currentIndex + 1} - ${currentIndex + currentSessionSize})`);

        let launchArgs = [
            '--autoplay-policy=no-user-gesture-required',
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-zygote',
            '--single-process',
            '--disable-software-rasterizer',
            '--no-first-run'
        ];

        const proxy = pickProxy();
        if (proxy) launchArgs.push(`--proxy-server=${proxy}`);

        console.log(`  🌐 Browser (proxy: ${proxy || 'DIRECT'})`);

        let browser;
        try {
            browser = await puppeteer.launch({
                headless: 'new',
                args: launchArgs,
                executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || null
            });
        } catch (launchError) {
            console.error(`  ❌ Failed to launch browser: ${launchError.message}`);
            await sleep(5000);
            try {
                browser = await puppeteer.launch({ headless: 'new', args: launchArgs });
            } catch (retryError) {
                console.error(`  ❌ Failed to launch browser on retry. Exiting.`);
                process.exit(1);
            }
        }

        let sessionProcessed = 0;
        let blocked = false;
        // Reset adaptive counter for each browser session
        consecutiveSuccessBatches = 0;

        while (sessionProcessed < currentSessionSize && !blocked) {
            const batchSize = Math.min(CONCURRENT_PAGES, currentSessionSize - sessionProcessed);
            const batch = toProcess.slice(currentIndex, currentIndex + batchSize);

            console.log(`📦 Batch ${currentIndex + 1}-${currentIndex + batchSize} / ${toProcess.length}`);

            try {
                // Stagger page loads to avoid blocks - add delay between each concurrent page
                const results = await Promise.all(batch.map(async (item, index) => {
                    // Add random delay before starting each page (staggered)
                    if (index > 0) {
                        const staggerDelay = PAGE_LOAD_DELAY_MIN + Math.random() * (PAGE_LOAD_DELAY_MAX - PAGE_LOAD_DELAY_MIN);
                        await sleep(staggerDelay * index); // Each page waits progressively longer
                    }
                    const data = await extractWithRetry(item, browser);
                    return {
                        rowIndex: item.rowIndex,
                        appName: data.appName
                    };
                }));

                results.forEach(r => {
                    console.log(`  → Row ${r.rowIndex + 1}: App Name = ${r.appName}`);
                });

                // Separate successful results from blocked ones (for logging)
                const successfulResults = results.filter(r => r.appName !== 'BLOCKED');
                const blockedResults = results.filter(r => r.appName === 'BLOCKED');

                // WRITE ALL RESULTS TO SHEET (including blocked ones)
                // This ensures blocked rows get marked and won't be reprocessed
                if (results.length > 0) {
                    await batchWriteToSheet(sheets, results);
                    console.log(`  ✅ Wrote ${results.length} results to sheet (${successfulResults.length} successful, ${blockedResults.length} blocked)`);

                    // Add cooldown after each write to prevent rate limits
                    await sleep(1000 + Math.random() * 1000); // 1-2 second cooldown
                }

                // Progress status every 10 batches
                if ((currentIndex / batchSize) % 10 === 0) {
                    const elapsed = Math.floor((Date.now() - sessionStartTime) / 60000);
                    const remaining = toProcess.length - currentIndex;
                    console.log(`\n📊 PROGRESS: ${currentIndex}/${toProcess.length} processed (${remaining} remaining) | Runtime: ${elapsed} mins\n`);
                }

                // If any results were blocked, mark for browser rotation
                if (blockedResults.length > 0) {
                    console.log(`  🛑 Block detected (${blockedResults.length} blocked, ${successfulResults.length} successful). Closing browser and rotating...`);
                    proxyStats.totalBlocks++;
                    proxyStats.perProxy[proxy || 'DIRECT'] = (proxyStats.perProxy[proxy || 'DIRECT'] || 0) + 1;
                    blocked = true;
                    consecutiveSuccessBatches = 0; // Reset on block
                } else {
                    consecutiveSuccessBatches++; // Track successful batches
                }

                // Update index for all processed items (both successful and blocked)
                currentIndex += batchSize;
                sessionProcessed += batchSize;
            } catch (err) {
                console.error(`  ❌ Batch error: ${err.message}`);
                currentIndex += batchSize;
                sessionProcessed += batchSize;
            }

            if (!blocked) {
                // Adaptive delay: reduce delay if we're having success (faster processing)
                const adaptiveMultiplier = Math.max(0.7, 1 - (consecutiveSuccessBatches * 0.05)); // Reduce delay by 5% per successful batch, min 70%
                const adjustedMin = BATCH_DELAY_MIN * adaptiveMultiplier;
                const adjustedMax = BATCH_DELAY_MAX * adaptiveMultiplier;
                const batchDelay = adjustedMin + Math.random() * (adjustedMax - adjustedMin);
                console.log(`  ⏳ Waiting ${Math.round(batchDelay / 1000)}s... (adaptive: ${Math.round(adaptiveMultiplier * 100)}%)`);
                await sleep(batchDelay);
            }
        }

        try {
            await browser.close();
            await sleep(2000);
        } catch (e) { }

        if (blocked) {
            const wait = PROXY_RETRY_DELAY_MIN + Math.random() * (PROXY_RETRY_DELAY_MAX - PROXY_RETRY_DELAY_MIN);
            console.log(`  ⏳ Block wait: ${Math.round(wait / 1000)}s...`);
            await sleep(wait);
        }
    }

    const remaining = await getUrlData(sheets);
    if (remaining.length > 0) {
        console.log(`📈 ${remaining.length} rows remaining for next scheduled run.`);
    }

    console.log('🔍 Proxy stats:', JSON.stringify(proxyStats));
    console.log('\n🏁 Complete.');
    process.exit(0);
})();
