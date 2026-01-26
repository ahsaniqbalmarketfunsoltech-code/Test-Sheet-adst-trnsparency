/**
 * TEXT ADS EXTRACTION AGENT (TOP TO BOTTOM)
 * =====================================
 * Extracts App Name & Subtitle from ALL Google Ads Transparency URLs
 * Processes rows from TOP to BOTTOM in batches
 * 
 * Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL
 *   Column C: App Link
 *   Column D: App Name
 *   Column E: App Headline
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
const SPREADSHEET_ID = '1yq2UwI94lwfYPY86CFwGbBsm3kpdqKrefYgrw3lEAwk';
const SHEET_NAME = process.env.SHEET_NAME || 'Text Ads data'; // Can be overridden via env var
// Escape sheet name for use in A1 notation (wrap in single quotes if it contains spaces)
const ESCAPED_SHEET_NAME = SHEET_NAME.includes(' ') ? `'${SHEET_NAME}'` : SHEET_NAME;
const CREDENTIALS_PATH = './credentials.json';
const SHEET_BATCH_SIZE = parseInt(process.env.SHEET_BATCH_SIZE) || 10000; // Rows to load per batch
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 2; // Reduced for reliability
const MAX_WAIT_TIME = 90000; // Increased timeout for slow loads
const MAX_RETRIES = 5; // More retries for better success rate
const POST_CLICK_WAIT = 8000;
const RETRY_WAIT_MULTIPLIER = 1.5; // Slower backoff
const PAGE_LOAD_DELAY_MIN = parseInt(process.env.PAGE_LOAD_DELAY_MIN) || 2000; // Increased delays
const PAGE_LOAD_DELAY_MAX = parseInt(process.env.PAGE_LOAD_DELAY_MAX) || 5000;

const BATCH_DELAY_MIN = parseInt(process.env.BATCH_DELAY_MIN) || 8000; // Increased for stability
const BATCH_DELAY_MAX = parseInt(process.env.BATCH_DELAY_MAX) || 15000; // Increased for stability

const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];
const MAX_PROXY_ATTEMPTS = parseInt(process.env.MAX_PROXY_ATTEMPTS) || Math.max(3, PROXIES.length);
const PROXY_RETRY_DELAY_MIN = parseInt(process.env.PROXY_RETRY_DELAY_MIN) || 45000; // Increased cooldown
const PROXY_RETRY_DELAY_MAX = parseInt(process.env.PROXY_RETRY_DELAY_MAX) || 120000; // Increased cooldown
const PAGES_PER_BROWSER = parseInt(process.env.PAGES_PER_BROWSER) || 15; // Fewer pages per browser for freshness

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

// OPTIMIZED: Stream batch size for reading (smaller = faster start, less memory)
const STREAM_BATCH_SIZE = parseInt(process.env.STREAM_BATCH_SIZE) || 500;

// Get total row count quickly (just metadata, no data scan)
async function getTotalRowCount(sheets) {
    try {
        const sheetMetadata = await sheets.spreadsheets.get({
            spreadsheetId: SPREADSHEET_ID,
            ranges: [`${ESCAPED_SHEET_NAME}`],
            fields: 'sheets.properties.gridProperties.rowCount'
        });
        const sheetProps = sheetMetadata.data.sheets?.[0]?.properties?.gridProperties;
        return sheetProps?.rowCount || 0;
    } catch (error) {
        console.error(`  ⚠️ Error getting row count: ${error.message}`);
        return 1000000; // Fallback to large number
    }
}

// OPTIMIZED: Get next batch of unprocessed rows (streaming approach)
// Returns { rows: [...], nextStartRow: number, hasMore: boolean }
async function getNextBatch(sheets, startRow, batchSize = STREAM_BATCH_SIZE) {
    const toProcess = [];

    try {
        const range = `${ESCAPED_SHEET_NAME}!A${startRow}:E${startRow + batchSize - 1}`;

        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: range,
        });

        const rows = response.data.values || [];

        if (rows.length === 0) {
            return { rows: [], nextStartRow: startRow, hasMore: false };
        }

        // Filter to only unprocessed rows
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const actualRowIndex = startRow + i - 1; // 0-indexed
            const url = row[1]?.trim() || '';
            const storeLink = row[2]?.trim() || '';
            const appName = row[3]?.trim() || '';
            const appSubtitle = row[4]?.trim() || '';

            // Skip if no URL
            if (!url) continue;

            // Skip rows that already have extracted data
            if (storeLink || appName || appSubtitle) continue;

            toProcess.push({
                url,
                rowIndex: actualRowIndex,
                needsMetadata: true,
                needsVideoId: false,
                existingStoreLink: ''
            });
        }

        return {
            rows: toProcess,
            nextStartRow: startRow + rows.length,
            hasMore: rows.length === batchSize,
            scannedCount: rows.length
        };
    } catch (error) {
        console.error(`  ⚠️ Error reading batch at row ${startRow}: ${error.message}`);
        return {
            rows: [],
            nextStartRow: startRow + batchSize,
            hasMore: true, // Assume more data, try next batch
            scannedCount: 0
        };
    }
}

// Legacy function for compatibility - now just returns empty (streaming is used instead)
async function getUrlData(sheets, batchSize = SHEET_BATCH_SIZE) {
    console.log(`📊 Using STREAMING mode - will process rows as they're loaded...`);
    return []; // Empty - we use streaming now
}

async function batchWriteToSheet(sheets, updates, retryCount = 0) {
    if (updates.length === 0) return;

    const MAX_WRITE_RETRIES = 5;
    const BASE_RETRY_DELAY = 5000; // 5 seconds base delay

    const data = [];
    updates.forEach(({ rowIndex, advertiserName, storeLink, appName, appSubtitle }) => {
        const rowNum = rowIndex + 1;

        // WRITE EVERYTHING - whatever data we get, write it to the sheet
        // This ensures every processed row gets marked with something

        // Write advertiser name (if we have any value)
        if (advertiserName && advertiserName !== 'NOT_FOUND') {
            data.push({ range: `${ESCAPED_SHEET_NAME}!A${rowNum}`, values: [[advertiserName]] });
        }

        // Write store link (always write something - even BLOCKED, NOT_FOUND, ERROR)
        const storeLinkValue = storeLink || 'NOT_FOUND';
        data.push({ range: `${ESCAPED_SHEET_NAME}!C${rowNum}`, values: [[storeLinkValue]] });

        // Write app name (always write something)
        const appNameValue = appName || 'NOT_FOUND';
        data.push({ range: `${ESCAPED_SHEET_NAME}!D${rowNum}`, values: [[appNameValue]] });

        // Write app subtitle/headline to Column E
        const appSubtitleValue = appSubtitle || 'NOT_FOUND';
        data.push({ range: `${ESCAPED_SHEET_NAME}!E${rowNum}`, values: [[appSubtitleValue]] });
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
// UNIFIED EXTRACTION - ONE VISIT PER URL
// Both metadata + video ID extracted on same page
// ============================================
async function extractAllInOneVisit(url, browser, needsMetadata, needsVideoId, existingStoreLink, attempt = 1) {
    let page;
    let result = {
        advertiserName: 'NOT_FOUND',
        appName: 'NOT_FOUND',
        storeLink: 'NOT_FOUND',
        appSubtitle: 'NOT_FOUND'
    };

    // Create page with error handling for browser crashes
    try {
        page = await browser.newPage();
    } catch (pageErr) {
        console.error(`  ❌ Failed to create page: ${pageErr.message}`);
        return { advertiserName: 'ERROR', appName: 'ERROR', storeLink: 'ERROR', appSubtitle: 'ERROR' };
    }

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
        const blockedTypes = ['image', 'font', 'other'];
        const blockedPatterns = [
            'analytics', 'google-analytics', 'doubleclick',
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
            return { advertiserName: 'BLOCKED', appName: 'BLOCKED', storeLink: 'BLOCKED' };
        }

        // Wait for dynamic elements to settle - CRITICAL for reliability
        const baseWait = 6000 + Math.random() * 3000; // 6-9 seconds base wait
        const attemptMultiplier = Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait * attemptMultiplier);

        // Wait for network to be truly idle (no pending requests)
        try {
            await page.waitForNetworkIdle({ idleTime: 2000, timeout: 15000 });
        } catch (e) {
            // Continue even if timeout - page may still have data
            console.log(`    ⚠️ Network idle timeout, continuing...`);
        }

        // Additional wait specifically for iframes to render (critical for Play Store links)
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
                                setTimeout(resolve, 2500); // Extra time after all iframes load
                            }
                        };
                        iframes.forEach(iframe => {
                            try {
                                if (iframe.contentDocument && iframe.contentDocument.readyState === 'complete') {
                                    checkLoaded();
                                } else {
                                    iframe.onload = checkLoaded;
                                    // Timeout after 6 seconds per iframe
                                    setTimeout(checkLoaded, 6000);
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
            // Extra buffer for iframe content to populate
            await sleep(1500);
        } catch (e) {
            // If iframe check fails, wait longer
            await sleep(2500);
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
        // PHASE 1: METADATA EXTRACTION
        // =====================================================
        let mainPageInfo = null;
        if (needsMetadata) {
            console.log(`  📊 Extracting metadata...`);

            mainPageInfo = await page.evaluate(() => {
                const getSafeText = (sel) => {
                    const el = document.querySelector(sel);
                    if (!el) return null;
                    const text = el.innerText.trim();
                    const blacklistWords = ['ad details', 'google ads', 'transparency center', 'about this ad'];
                    if (!text || blacklistWords.some(word => text.toLowerCase().includes(word)) || text.length < 2) return null;
                    return text;
                };

                const advertiserSelectors = [
                    '.advertiser-name',
                    '.advertiser-name-container',
                    'h1',
                    '.creative-details-page-header-text',
                    '.ad-details-heading'
                ];

                let advertiserName = null;
                for (const sel of advertiserSelectors) {
                    advertiserName = getSafeText(sel);
                    if (advertiserName) break;
                }

                const checkVideo = () => {
                    const videoEl = document.querySelector('video');
                    if (videoEl && videoEl.offsetWidth > 10 && videoEl.offsetHeight > 10) return true;
                    return document.body.innerText.includes('Format: Video');
                };

                return {
                    advertiserName: advertiserName || 'NOT_FOUND',
                    blacklist: advertiserName ? advertiserName.toLowerCase() : '',
                    isVideo: checkVideo()
                };
            });

            const blacklistName = mainPageInfo.blacklist;
            result.advertiserName = mainPageInfo.advertiserName;

            const frames = page.frames();
            for (const frame of frames) {
                try {
                    const frameData = await frame.evaluate((blacklist) => {
                        const data = { appName: null, storeLink: null, appSubtitle: null, isVideo: false };
                        const root = document.querySelector('#portrait-landscape-phone') || document.body;

                        // Check if this frame content is visible (has dimensions)
                        const bodyRect = document.body.getBoundingClientRect();
                        if (bodyRect.width < 50 || bodyRect.height < 50) {
                            return { ...data, isHidden: true };
                        }

                        // =====================================================
                        // ULTRA-PRECISE STORE LINK EXTRACTOR
                        // Only accepts REAL Play Store / App Store links
                        // OR constructs from package name found in DOM
                        // =====================================================

                        // Function to find package name in ANY text/attribute in the DOM
                        const findPackageName = () => {
                            // Package name pattern: com.something.something (at least 2 dots, valid chars)
                            const packageRegex = /\b(com\.[a-zA-Z][a-zA-Z0-9_]*\.[a-zA-Z0-9_.]+)\b/g;
                            const altPackageRegex = /\b([a-zA-Z][a-zA-Z0-9_]*\.[a-zA-Z][a-zA-Z0-9_]*\.[a-zA-Z0-9_.]+)\b/g;

                            const foundPackages = new Set();

                            // 1. Search in ALL href attributes
                            const allLinks = root.querySelectorAll('a[href]');
                            for (const link of allLinks) {
                                const href = link.href || '';
                                // Direct id= parameter
                                const idMatch = href.match(/[?&]id=([a-zA-Z][a-zA-Z0-9_.]+)/);
                                if (idMatch && idMatch[1] && idMatch[1].includes('.')) {
                                    foundPackages.add(idMatch[1]);
                                }
                                // Package in URL path
                                const matches = href.match(packageRegex);
                                if (matches) matches.forEach(m => foundPackages.add(m));
                            }

                            // 2. Search in ALL data-* attributes
                            const allElements = root.querySelectorAll('*');
                            for (const el of allElements) {
                                // Check all attributes
                                for (const attr of el.attributes || []) {
                                    const val = attr.value || '';
                                    const matches = val.match(packageRegex);
                                    if (matches) matches.forEach(m => foundPackages.add(m));
                                    // Also check for id= pattern
                                    const idMatch = val.match(/[?&]id=([a-zA-Z][a-zA-Z0-9_.]+)/);
                                    if (idMatch && idMatch[1] && idMatch[1].includes('.')) {
                                        foundPackages.add(idMatch[1]);
                                    }
                                }
                            }

                            // 3. Search in script tags content
                            const scripts = root.querySelectorAll('script');
                            for (const script of scripts) {
                                const content = script.textContent || '';
                                const matches = content.match(packageRegex);
                                if (matches) matches.forEach(m => foundPackages.add(m));
                            }

                            // 4. Search in inline styles and other text
                            const html = root.innerHTML || '';
                            const htmlMatches = html.match(packageRegex);
                            if (htmlMatches) htmlMatches.forEach(m => foundPackages.add(m));

                            // Filter out false positives (CSS classes, common patterns)
                            const blacklistPatterns = ['com.google.android', 'com.android.', 'schema.org', 'w3.org'];
                            const validPackages = [...foundPackages].filter(pkg => {
                                if (pkg.length < 5 || pkg.length > 100) return false;
                                if (blacklistPatterns.some(bp => pkg.startsWith(bp))) return false;
                                if (pkg.split('.').length < 2) return false;
                                return true;
                            });

                            // Return first valid package (prioritize com.* packages)
                            const comPackages = validPackages.filter(p => p.startsWith('com.'));
                            return comPackages[0] || validPackages[0] || null;
                        };

                        // Build Play Store URL from package name
                        const buildPlayStoreUrl = (packageName) => {
                            if (!packageName) return null;
                            return `https://play.google.com/store/apps/details?id=${packageName}`;
                        };
                        // =====================================================
                        const extractStoreLink = (href) => {
                            if (!href || typeof href !== 'string') return null;
                            if (href.includes('javascript:') || href === '#') return null;

                            const isValidStoreLink = (url) => {
                                if (!url) return false;
                                const isPlayStore = url.includes('play.google.com/store/apps') && url.includes('id=');
                                const isAppStore = (url.includes('apps.apple.com') || url.includes('itunes.apple.com')) && url.includes('/app/');
                                return isPlayStore || isAppStore;
                            };

                            if (isValidStoreLink(href)) return href;

                            if (href.includes('googleadservices.com') || href.includes('/pagead/aclk')) {
                                try {
                                    const patterns = [
                                        /[?&]adurl=([^&\s]+)/i,
                                        /[?&]dest=([^&\s]+)/i,
                                        /[?&]url=([^&\s]+)/i
                                    ];
                                    for (const pattern of patterns) {
                                        const match = href.match(pattern);
                                        if (match && match[1]) {
                                            const decoded = decodeURIComponent(match[1]);
                                            if (isValidStoreLink(decoded)) return decoded;
                                        }
                                    }
                                } catch (e) { }
                            }

                            try {
                                const playMatch = href.match(/(https?:\/\/play\.google\.com\/store\/apps\/details\?id=[a-zA-Z0-9._]+)/);
                                if (playMatch && playMatch[1]) return playMatch[1];
                                const appMatch = href.match(/(https?:\/\/(apps|itunes)\.apple\.com\/[^\s&"']+\/app\/[^\s&"']+)/);
                                if (appMatch && appMatch[1]) return appMatch[1];

                                // Try to extract package name from href and build URL
                                const pkgMatch = href.match(/[?&]id=([a-zA-Z][a-zA-Z0-9_.]+)/);
                                if (pkgMatch && pkgMatch[1] && pkgMatch[1].includes('.')) {
                                    return buildPlayStoreUrl(pkgMatch[1]);
                                }
                            } catch (e) { }

                            return null;
                        };

                        // =====================================================
                        // CLEAN APP NAME - Unicode/Multi-language safe
                        // =====================================================
                        const cleanAppName = (text) => {
                            if (!text || typeof text !== 'string') return null;
                            let clean = text.trim();
                            // Remove invisible Unicode characters only
                            clean = clean.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069\u00AD]/g, '');
                            // Remove CSS-like patterns (only ASCII)
                            clean = clean.replace(/\.[a-zA-Z][\w-]*/g, ' ');
                            clean = clean.replace(/[a-zA-Z-]+\s*:\s*[^;]+;/g, ' ');
                            // Remove markers
                            clean = clean.split('!@~!@~')[0];
                            if (clean.includes('|')) {
                                const parts = clean.split('|').map(p => p.trim()).filter(p => p.length > 2);
                                if (parts.length > 0) clean = parts[0];
                            }
                            clean = clean.replace(/\s+/g, ' ').trim();
                            if (clean.length < 1 || clean.length > 100) return null;
                            // Only reject if it's ONLY ASCII digits/punctuation (keep ALL Unicode letters)
                            if (/^[\d\s.,!?@#$%^&*()\-_=+\[\]{}|\\;:'"<>\/`~]+$/.test(clean)) return null;
                            // Reject common non-app-name text
                            const lower = clean.toLowerCase();
                            if (lower === 'install' || lower === 'open' || lower === 'get' || lower === 'download') return null;
                            return clean;
                        };

                        // =====================================================
                        // EXTRACTION - Find app name using KDwhZb class (primary)
                        // Then verify with store link selectors
                        // =====================================================

                        // PRIMARY: Use the KDwhZb div class to find app name (contains span with name)

                        const appNameDivSelectors = [
                            'div.KDwhZb-Gxk8ed-r4nke',
                            'div[class*="KDwhZb-Gxk8ed-r4nke"]',
                            'div[class*="KDwhZb"][class*="Gxk8ed"]',
                            'div.cS4Vcb-kb9wTc',
                            'div[class*="cS4Vcb-kb9wTc"]',
                            'div[class*="cS4Vcb-pGL6qe-c0XB9d"]',
                            // Additional selectors for multi-language support
                            '[data-asoch-targets*="AppName"]',
                            '[data-asoch-targets*="appName"]',
                            '[data-asoch-targets*="app_name"]',
                            '.app-name',
                            '[class*="app-name"]',
                            '[class*="appName"]',
                            '[class*="title"][class*="app"]',
                            'div[role="heading"]',
                            'span[role="heading"]'
                        ];

                        // First try to get app name from the KDwhZb div (span inside)
                        for (const selector of appNameDivSelectors) {
                            const elements = root.querySelectorAll(selector);
                            for (const el of elements) {
                                // Get text from span inside or the div itself
                                const span = el.querySelector('span');
                                const rawName = span ? (span.innerText || span.textContent || '') : (el.innerText || el.textContent || '');
                                const appName = cleanAppName(rawName);
                                if (appName && appName.toLowerCase() !== blacklist && appName.length > 2) {
                                    data.appName = appName;
                                    break;
                                }
                            }
                            if (data.appName) break;
                        }

                        // SECONDARY: Link-based selectors (for store link + fallback app name)
                        const appLinkSelectors = [
                            'a[data-asoch-targets*="ochAppName"]',
                            'a[data-asoch-targets*="appname" i]',
                            'a[data-asoch-targets*="rrappname" i]',
                            'a[class*="short-app-name"]',
                            '.short-app-name a'
                        ];

                        for (const selector of appLinkSelectors) {
                            const elements = root.querySelectorAll(selector);
                            for (const el of elements) {
                                const storeLink = extractStoreLink(el.href);

                                // If we don't have app name yet, try to get from link text
                                if (!data.appName) {
                                    const rawName = el.innerText || el.textContent || '';
                                    const appName = cleanAppName(rawName);
                                    if (appName && appName.toLowerCase() !== blacklist) {
                                        data.appName = appName;
                                    }
                                }

                                // If we have both name and link, return immediately
                                if (data.appName && storeLink) {
                                    return { appName: data.appName, storeLink, appSubtitle: data.appSubtitle, isVideo: true, isHidden: false };
                                } else if (storeLink && !data.storeLink) {
                                    data.storeLink = storeLink;
                                }
                            }
                        }

                        // Backup: Install button for link
                        if (!data.storeLink) {
                            const installSels = [
                                'a[data-asoch-targets*="ochButton"]',
                                'a[data-asoch-targets*="Install" i]',
                                'a[aria-label*="Install" i]',
                                'a[href*="play.google.com"]',
                                'a[href*="apps.apple.com"]'
                            ];
                            for (const sel of installSels) {
                                const el = root.querySelector(sel);
                                if (el && el.href) {
                                    const storeLink = extractStoreLink(el.href);
                                    if (storeLink) {
                                        data.storeLink = storeLink;
                                        data.isVideo = true;
                                        break;
                                    }
                                }
                            }
                        }

                        // =====================================================
                        // ULTIMATE FALLBACK: Search ENTIRE DOM for package name
                        // If no store link found, search every tag/attribute
                        // =====================================================
                        if (!data.storeLink) {
                            const packageName = findPackageName();
                            if (packageName) {
                                data.storeLink = buildPlayStoreUrl(packageName);
                                console.log('Found package via DOM search:', packageName);
                            }
                        }

                        // Fallback for app name only (if primary selectors didn't find it)
                        if (!data.appName) {
                            const textSels = [
                                'div[class*="KDwhZb"] span',
                                '[role="heading"]',
                                'div[class*="app-name"]',
                                '.app-title',
                                // More fallback selectors
                                '[class*="title"]',
                                'h1', 'h2', 'h3',
                                'strong',
                                '[class*="name"]'
                            ];
                            for (const sel of textSels) {
                                const elements = root.querySelectorAll(sel);
                                for (const el of elements) {
                                    const rawName = el.innerText || el.textContent || '';
                                    const appName = cleanAppName(rawName);
                                    // Accept any language - just check it's not the advertiser name
                                    if (appName && appName.toLowerCase() !== blacklist &&
                                        !appName.toLowerCase().includes('ad details') &&
                                        !appName.toLowerCase().includes('google ads') &&
                                        appName.length > 1) {
                                        data.appName = appName;
                                        break;
                                    }
                                }
                                if (data.appName) break;
                            }
                        }

                        // LAST RESORT: Use subtitle as app name if we have subtitle but no name
                        // (sometimes the app name is only in subtitle position)

                        // =====================================================
                        // EXTRACT APP SUBTITLE/HEADLINE using cS4Vcb-vnv8ic class
                        // =====================================================
                        if (!data.appSubtitle) {
                            const subtitleSelectors = [
                                '.cS4Vcb-vnv8ic',
                                '[class*="cS4Vcb-vnv8ic"]',
                                'div.cS4Vcb-vnv8ic',
                                '[class*="vnv8ic"]',
                                '[data-asoch-targets*="Headline"]',
                                '[data-asoch-targets*="Description"]',
                                '.description',
                                'div[class*="description"]',
                                // Additional subtitle selectors
                                '[class*="subtitle"]',
                                '[class*="tagline"]',
                                'p',
                                'span[class*="text"]'
                            ];
                            for (const sel of subtitleSelectors) {
                                const els = root.querySelectorAll(sel);
                                for (const el of els) {
                                    let text = (el.innerText || el.textContent || '').trim();
                                    // Only remove invisible characters, keep all language text
                                    text = text.replace(/[\u200B-\u200D\uFEFF\u00AD]/g, '');
                                    // Skip if same as app name or too short/long
                                    if (text && text.length > 2 && text.length < 200 && text !== data.appName) {
                                        // Filter out button text only (keep all language text)
                                        const lower = text.toLowerCase();
                                        const blacklistSub = ['install', 'open', 'download', 'play', 'get', 'ad details', 'google play', 'app store'];
                                        if (!blacklistSub.some(b => lower === b || lower === b + ' now')) {
                                            data.appSubtitle = text;
                                            break;
                                        }
                                    }
                                }
                                if (data.appSubtitle) break;
                            }
                        }

                        // If we still don't have app name but have subtitle, use part of subtitle
                        if (!data.appName && data.appSubtitle) {
                            // Take first sentence or first 50 chars as potential app name
                            let potentialName = data.appSubtitle.split(/[.!?،。！？]/)[0].trim();
                            if (potentialName.length > 50) potentialName = potentialName.substring(0, 50).trim();
                            if (potentialName.length > 2) {
                                data.appName = potentialName;
                            }
                        }

                        data.isHidden = false;
                        return data;
                    }, blacklistName);

                    // Skip hidden frames
                    if (frameData.isHidden) continue;

                    // Capture subtitle if found
                    if (frameData.appSubtitle && result.appSubtitle === 'NOT_FOUND') {
                        result.appSubtitle = frameData.appSubtitle;
                    }

                    // If we found BOTH app name AND store link, use this immediately (high confidence)
                    if (frameData.appName && frameData.storeLink && result.appName === 'NOT_FOUND') {
                        result.appName = cleanName(frameData.appName);
                        result.storeLink = frameData.storeLink;
                        console.log(`  ✓ Found: ${result.appName} -> ${result.storeLink.substring(0, 60)}...`);
                        if (result.appSubtitle !== 'NOT_FOUND') {
                            console.log(`  ✓ Subtitle: ${result.appSubtitle}`);
                        }
                        break; // We have both, stop searching
                    }

                    // If we only found name (no link), store it but keep looking
                    if (frameData.appName && !frameData.storeLink && result.appName === 'NOT_FOUND') {
                        result.appName = cleanName(frameData.appName);
                        // DON'T break - continue looking for a frame with BOTH name+link
                    }
                } catch (e) { }
            }

            // Final fallback from Meta/Title
            if (result.appName === 'NOT_FOUND' || result.appName === 'Ad Details') {
                try {
                    const title = await page.title();
                    if (title && !title.toLowerCase().includes('google ads')) {
                        result.appName = title.split(' - ')[0].split('|')[0].trim();
                    }
                } catch (e) { }
            }
        }

        await page.close();
        return result;
    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        await page.close();
        return { advertiserName: 'ERROR', appName: 'ERROR', storeLink: 'ERROR', appSubtitle: 'ERROR' };
    }
}

async function extractWithRetry(item, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        if (attempt > 1) console.log(`  🔄 Retry ${attempt}/${MAX_RETRIES}...`);

        const data = await extractAllInOneVisit(
            item.url,
            browser,
            item.needsMetadata,
            item.needsVideoId,
            item.existingStoreLink,
            attempt
        );

        if (data.storeLink === 'BLOCKED' || data.appName === 'BLOCKED') return data;

        // Success criteria: found at least app name or store link
        const success = data.appName !== 'NOT_FOUND' || data.storeLink !== 'NOT_FOUND';

        if (success) {
            return data;
        } else {
            console.log(`  ⚠️ Attempt ${attempt} - no data found. Retrying...`);
        }

        await randomDelay(2000, 4000);
    }
    // If we're here, we exhausted retries. Return whatever we have.
    return { advertiserName: 'NOT_FOUND', storeLink: 'NOT_FOUND', appName: 'NOT_FOUND', appSubtitle: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION - OPTIMIZED STREAMING MODE
// ============================================
(async () => {
    console.log(`🤖 Starting Text Ads Extraction Agent (TOP TO BOTTOM - STREAMING)...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Columns: A=Advertiser, B=URL, C=App Link, D=App Name, E=Headline`);
    console.log(`🚀 Mode: STREAMING (processes immediately, minimal memory)\n`);

    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000; // 5.5 hours

    const sheets = await getGoogleSheetsClient();

    // Get total row count quickly (just metadata call)
    const totalRows = await getTotalRowCount(sheets);
    console.log(`📊 Sheet has ${totalRows} total rows`);
    console.log(` Starting from row 2, processing to row ${totalRows}\n`);

    console.log(PROXIES.length ? `🔁 Proxy rotation enabled (${PROXIES.length} proxies)` : '🔁 Running direct');

    let currentRow = 2; // Start from row 2 (skip header)
    let totalProcessed = 0;
    let totalScanned = 0;
    let totalNotFoundCount = 0;
    let consecutiveSuccessBatches = 0;
    let consecutiveEmptyScanIterations = 0;
    const MAX_EMPTY_SCANS = 10;
    let rowsQueue = []; // Queue for unprocessed rows found during streaming

    while (currentRow <= totalRows || rowsQueue.length > 0) {
        if (Date.now() - sessionStartTime > MAX_RUNTIME) {
            console.log('\n⏰ Time limit reached. Stopping.');
            break;
        }

        // 🏢 Start a new browser session
        console.log(`\n🏢 Starting Browser Session (Current Row: ${currentRow}, Queue: ${rowsQueue.length})`);

        let launchArgs = [
            '--autoplay-policy=no-user-gesture-required',
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-software-rasterizer',
            '--no-first-run',
            '--disable-extensions',
            '--disable-background-networking',
            '--disable-sync',
            '--disable-translate',
            '--metrics-recording-only',
            '--disable-default-apps',
            '--mute-audio',
            '--incognito'
        ];

        const proxy = pickProxy();
        if (proxy) launchArgs.push(`--proxy-server=${proxy}`);
        console.log(`  🌐 Proxy: ${proxy || 'DIRECT'}`);

        let browser;
        try {
            browser = await puppeteer.launch({ headless: 'new', args: launchArgs, executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || null });
        } catch (e) {
            console.error(`  ❌ Launch failed: ${e.message}`);
            await sleep(5000);
            continue;
        }

        let sessionProcessed = 0;
        let blocked = false;

        // Process up to PAGES_PER_BROWSER in this browser context
        while (sessionProcessed < PAGES_PER_BROWSER && !blocked) {
            // 1. Refill rowsQueue if it's empty
            if (rowsQueue.length === 0 && currentRow <= totalRows) {
                console.log(`  📥 Scanning next ${STREAM_BATCH_SIZE} rows starting at ${currentRow}...`);
                const batchResult = await getNextBatch(sheets, currentRow, STREAM_BATCH_SIZE);

                totalScanned += (batchResult.scannedCount || 0);
                currentRow = batchResult.nextStartRow;
                rowsQueue = batchResult.rows;

                if (rowsQueue.length === 0) {
                    if (!batchResult.hasMore) {
                        consecutiveEmptyScanIterations++;
                        if (consecutiveEmptyScanIterations >= MAX_EMPTY_SCANS) {
                            console.log(`  ✨ Reached end of data.`);
                            break;
                        }
                    }
                    console.log(`  ⏭️ No work found in this chunk, scanning next...`);
                    continue;
                }
                consecutiveEmptyScanIterations = 0;
                console.log(`  ✓ Found ${rowsQueue.length} unprocessed rows in this chunk.`);
            }

            // If we have nothing left to do and queue is still empty
            if (rowsQueue.length === 0) break;

            // 2. Take a batch from the queue to process concurrently
            const batchToProcess = rowsQueue.splice(0, Math.min(CONCURRENT_PAGES, rowsQueue.length));
            console.log(`📦 Processing batch of ${batchToProcess.length} rows (${rowsQueue.length} remaining in queue)...`);

            if (!browser.isConnected()) {
                console.log(`  ⚠️ Browser disconnected.`);
                blocked = true;
                break;
            }

            // 3. Extract data for these rows
            const results = [];
            for (let i = 0; i < batchToProcess.length; i++) {
                const item = batchToProcess[i];
                if (i > 0) await sleep(PAGE_LOAD_DELAY_MIN + Math.random() * 1000);

                try {
                    const data = await extractWithRetry(item, browser);
                    results.push({ ...data, rowIndex: item.rowIndex });
                } catch (err) {
                    console.error(`  ❌ Row ${item.rowIndex + 1} error: ${err.message}`);
                    results.push({ rowIndex: item.rowIndex, advertiserName: 'ERROR', storeLink: 'ERROR', appName: 'ERROR', appSubtitle: 'ERROR' });
                }
            }

            // 4. Log and Write results
            results.forEach(r => console.log(`  → Row ${r.rowIndex + 1}: ${r.appName} | ${r.appSubtitle?.substring(0, 30)}...`));

            if (results.length > 0) {
                await batchWriteToSheet(sheets, results);
                totalProcessed += results.length;
                sessionProcessed += results.length;

                const blockedResults = results.filter(r => r.storeLink === 'BLOCKED' || r.appName === 'BLOCKED');
                if (blockedResults.length > 0) {
                    console.log(`  🛑 Block detected. Rotating browser...`);
                    blocked = true;
                }

                const progress = Math.round((currentRow / totalRows) * 100);
                const elapsed = Math.floor((Date.now() - sessionStartTime) / 60000);
                console.log(`  ✅ Wrote results. Progress: ~${progress}% | Runtime: ${elapsed}m\n`);
            }

            if (!blocked) await sleep(BATCH_DELAY_MIN + Math.random() * 2000);
        }

        // Cleanup browser
        try { await browser.close(); } catch (e) { }

        // Block cooldown
        if (blocked) {
            const wait = PROXY_RETRY_DELAY_MIN + Math.random() * (PROXY_RETRY_DELAY_MAX - PROXY_RETRY_DELAY_MIN);
            console.log(`  ⏳ Block cooldown: ${Math.round(wait / 1000)}s...`);
            await sleep(wait);
        }

        // Force GC if possible
        if (global.gc) { try { global.gc(); } catch (e) { } }
    }

    // Final stats
    console.log('\n' + '='.repeat(50));
    console.log('📈 FINAL STATS:');
    console.log(`   Rows scanned: ${totalScanned}`);
    console.log(`   Rows processed: ${totalProcessed}`);
    console.log(`   NOT_FOUND count: ${totalNotFoundCount}`);
    console.log(`   Last row reached: ${currentRow}`);
    console.log('🔍 Proxy stats:', JSON.stringify(proxyStats));
    console.log('🏁 Complete.');
    process.exit(0);
})();

