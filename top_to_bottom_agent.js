/**
 * APP NAME EXTRACTION AGENT (TOP TO BOTTOM)
 * =====================================
 * Extracts App Name & Subtitle from ALL Google Ads Transparency URLs
 * Processes rows from TOP to BOTTOM in batches
 * 
 * Run this alongside "unified_agent copy.js" (bottom-to-top) for 2x speed!
 * 
 * Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL
 *   Column C: App Link
 *   Column D: App Name
 *   Column F: App Subtitle/Headline
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

/**
 * GET URL DATA - TOP TO BOTTOM
 * Scans from row 2 (first data row) downward
 */
async function getUrlData(sheets, batchSize = SHEET_BATCH_SIZE) {
    const toProcess = [];

    console.log(`📊 Finding total rows and scanning ALL data from TOP to BOTTOM in batches of ${batchSize} rows...`);

    // Get the actual total row count using sheet metadata
    let totalRows = 0;
    try {
        const sheetMetadata = await sheets.spreadsheets.get({
            spreadsheetId: SPREADSHEET_ID,
            ranges: [`${ESCAPED_SHEET_NAME}`],
            fields: 'sheets.properties.gridProperties.rowCount'
        });

        const sheetProps = sheetMetadata.data.sheets?.[0]?.properties?.gridProperties;
        if (sheetProps && sheetProps.rowCount) {
            totalRows = sheetProps.rowCount;
            console.log(`  ✓ Sheet metadata indicates ${totalRows} total rows`);
        }

        // Find actual last row with data
        if (totalRows > 0) {
            let foundLastDataRow = false;
            let checkEnd = totalRows;

            while (!foundLastDataRow && checkEnd > 1) {
                const checkStart = Math.max(2, checkEnd - 1000);
                try {
                    const checkResponse = await sheets.spreadsheets.values.get({
                        spreadsheetId: SPREADSHEET_ID,
                        range: `${ESCAPED_SHEET_NAME}!B${checkStart}:B${checkEnd}`,
                    });
                    const checkRows = checkResponse.data.values || [];

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
                    foundLastDataRow = true;
                }
            }
        }

        console.log(`  ✓ Will scan from TOP (row 2) to BOTTOM (row ${totalRows})`);
    } catch (error) {
        console.error(`  ⚠️ Error finding total rows: ${error.message}`);
        totalRows = 100000;
    }

    if (totalRows <= 1) {
        console.log(`📊 No data rows found\n`);
        return toProcess;
    }

    // =====================================================
    // PROCESS FROM TOP TO BOTTOM
    // =====================================================
    let startRow = 2; // Start from row 2 (skip header)
    let hasMoreData = true;
    let totalProcessed = 0;

    while (hasMoreData && startRow <= totalRows) {
        try {
            const endRow = Math.min(startRow + batchSize - 1, totalRows);
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

            // Process rows in order (TOP to BOTTOM)
            for (let i = 0; i < rows.length; i++) {
                const row = rows[i];
                const actualRowIndex = startRow + i - 1; // 0-indexed row number
                const url = row[1]?.trim() || '';
                const storeLink = row[2]?.trim() || '';
                const appName = row[3]?.trim() || '';
                const videoId = row[4]?.trim() || '';

                // Skip if no URL
                if (!url) continue;

                // Skip rows that already have ANY value in column C (storeLink)
                if (storeLink) {
                    const hasValidStoreLink = storeLink.includes('play.google.com') || storeLink.includes('apps.apple.com');
                    const needsVideoId = hasValidStoreLink && !videoId;

                    if (needsVideoId) {
                        toProcess.push({
                            url,
                            rowIndex: actualRowIndex,
                            needsMetadata: false,
                            needsVideoId: true,
                            existingStoreLink: storeLink
                        });
                    }
                    continue;
                }

                // Row has no storeLink - needs processing
                toProcess.push({
                    url,
                    rowIndex: actualRowIndex,
                    needsMetadata: true,
                    needsVideoId: false,
                    existingStoreLink: ''
                });
            }

            totalProcessed += rows.length;
            console.log(`  ✓ Processed ${totalProcessed} rows (from top), found ${toProcess.length} to process`);

            // Move to next batch (going forward)
            startRow = endRow + 1;

            if (startRow > totalRows) {
                hasMoreData = false;
            } else {
                await sleep(100);
            }
        } catch (error) {
            console.error(`  ⚠️ Error loading batch starting at row ${startRow}: ${error.message}`);
            startRow += batchSize;
            if (startRow > totalRows) {
                hasMoreData = false;
            }
            await sleep(500);
        }
    }

    console.log(`📊 Total: ${totalProcessed} rows scanned, ${toProcess.length} need processing (from top to bottom)\n`);
    return toProcess;
}

async function batchWriteToSheet(sheets, updates, retryCount = 0) {
    if (updates.length === 0) return;

    const MAX_WRITE_RETRIES = 5;
    const BASE_RETRY_DELAY = 5000;

    const data = [];
    updates.forEach(({ rowIndex, advertiserName, storeLink, appName, appSubtitle }) => {
        const rowNum = rowIndex + 1;

        if (advertiserName && advertiserName !== 'NOT_FOUND') {
            data.push({ range: `${ESCAPED_SHEET_NAME}!A${rowNum}`, values: [[advertiserName]] });
        }

        const storeLinkValue = storeLink || 'NOT_FOUND';
        data.push({ range: `${ESCAPED_SHEET_NAME}!C${rowNum}`, values: [[storeLinkValue]] });

        const appNameValue = appName || 'NOT_FOUND';
        data.push({ range: `${ESCAPED_SHEET_NAME}!D${rowNum}`, values: [[appNameValue]] });

        const appSubtitleValue = appSubtitle || 'NOT_FOUND';
        data.push({ range: `${ESCAPED_SHEET_NAME}!F${rowNum}`, values: [[appSubtitleValue]] });

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
            const retryDelay = BASE_RETRY_DELAY * Math.pow(2, retryCount);
            console.log(`  ⚠️ Sheet write error (${errorMessage.substring(0, 50)}...). Retry ${retryCount + 1}/${MAX_WRITE_RETRIES} in ${retryDelay / 1000}s...`);
            await sleep(retryDelay);
            return batchWriteToSheet(sheets, updates, retryCount + 1);
        } else if (retryCount < MAX_WRITE_RETRIES) {
            const retryDelay = 3000;
            console.log(`  ⚠️ Sheet write error: ${errorMessage}. Retry ${retryCount + 1}/${MAX_WRITE_RETRIES} in ${retryDelay / 1000}s...`);
            await sleep(retryDelay);
            return batchWriteToSheet(sheets, updates, retryCount + 1);
        } else {
            console.error(`  ❌ Sheet write FAILED after ${MAX_WRITE_RETRIES} retries: ${errorMessage}`);
            const failedRows = updates.map(u => u.rowIndex + 1).join(', ');
            console.error(`  ❌ Failed rows: ${failedRows}`);
        }
    }
}

// ============================================
// UNIFIED EXTRACTION - ONE VISIT PER URL
// ============================================
async function extractAllInOneVisit(url, browser, needsMetadata, needsVideoId, existingStoreLink, attempt = 1) {
    const page = await browser.newPage();
    let result = {
        advertiserName: 'NOT_FOUND',
        appName: 'NOT_FOUND',
        storeLink: 'NOT_FOUND',
        appSubtitle: 'NOT_FOUND'
    };

    const cleanName = (name) => {
        if (!name) return 'NOT_FOUND';
        let cleaned = name.trim();
        cleaned = cleaned.replace(/[\u200B-\u200D\uFEFF\u2066-\u2069]/g, '');
        cleaned = cleaned.replace(/[a-zA-Z-]+\s*:\s*[^;]+;?/g, ' ');
        cleaned = cleaned.replace(/\d+px/g, ' ');
        cleaned = cleaned.replace(/\*+/g, ' ');
        cleaned = cleaned.replace(/\.[a-zA-Z][\w-]*/g, ' ');
        cleaned = cleaned.split('!@~!@~')[0];
        if (cleaned.includes('|')) {
            cleaned = cleaned.split('|')[0];
        }
        cleaned = cleaned.replace(/\s+/g, ' ').trim();
        if (cleaned.length < 2 || cleaned.length > 80) return 'NOT_FOUND';
        if (/:\s*\d/.test(cleaned) || cleaned.includes('height') || cleaned.includes('width') || cleaned.includes('font')) {
            return 'NOT_FOUND';
        }
        return cleaned || 'NOT_FOUND';
    };

    const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
    await page.setUserAgent(userAgent);

    const viewport = VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)];
    await page.setViewport(viewport);

    const screenWidth = viewport.width + Math.floor(Math.random() * 100) - 50;
    const screenHeight = viewport.height + Math.floor(Math.random() * 100) - 50;

    await page.evaluateOnNewDocument((screenW, screenH) => {
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5], configurable: true });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'], configurable: true });
        Object.defineProperty(navigator, 'platform', {
            get: () => /Win/.test(navigator.userAgent) ? 'Win32' : /Mac/.test(navigator.userAgent) ? 'MacIntel' : 'Linux x86_64',
            configurable: true
        });
        Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 + Math.floor(Math.random() * 4), configurable: true });
        Object.defineProperty(navigator, 'deviceMemory', { get: () => [4, 8, 16][Math.floor(Math.random() * 3)], configurable: true });
        Object.defineProperty(screen, 'width', { get: () => screenW, configurable: true });
        Object.defineProperty(screen, 'height', { get: () => screenH, configurable: true });
        Object.defineProperty(screen, 'availWidth', { get: () => screenW, configurable: true });
        Object.defineProperty(screen, 'availHeight', { get: () => screenH - 40, configurable: true });

        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
        );

        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function () {
            const context = this.getContext('2d');
            if (context) {
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {
                    imageData.data[i] += Math.random() * 0.01 - 0.005;
                }
                context.putImageData(imageData, 0, 0);
            }
            return originalToDataURL.apply(this, arguments);
        };
    }, screenWidth, screenHeight);

    await page.setRequestInterception(true);
    page.on('request', (request) => {
        const requestUrl = request.url();
        const resourceType = request.resourceType();
        const blockedTypes = ['image', 'font', 'other'];
        const blockedPatterns = ['analytics', 'google-analytics', 'doubleclick', 'facebook.com', 'bing.com', 'logs', 'collect', 'securepubads'];

        if (blockedTypes.includes(resourceType) || blockedPatterns.some(p => requestUrl.includes(p))) {
            request.abort();
        } else {
            request.continue();
        }
    });

    try {
        console.log(`  🚀 Loading (${viewport.width}x${viewport.height}): ${url.substring(0, 50)}...`);

        try {
            const client = await page.target().createCDPSession();
            await client.send('Input.dispatchMouseEvent', {
                type: 'mouseMoved',
                x: Math.random() * viewport.width,
                y: Math.random() * viewport.height
            });
        } catch (e) { }

        const acceptLanguages = ['en-US,en;q=0.9', 'en-US,en;q=0.9,zh-CN;q=0.8', 'en-US,en;q=0.9,fr;q=0.8', 'en-GB,en;q=0.9', 'en-US,en;q=0.9,es;q=0.8'];
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

        const baseWait = 4000 + Math.random() * 2000;
        const attemptMultiplier = Math.pow(RETRY_WAIT_MULTIPLIER, attempt - 1);
        await sleep(baseWait * attemptMultiplier);

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
                                setTimeout(resolve, 1500);
                            }
                        };
                        iframes.forEach(iframe => {
                            try {
                                if (iframe.contentDocument && iframe.contentDocument.readyState === 'complete') {
                                    checkLoaded();
                                } else {
                                    iframe.onload = checkLoaded;
                                    setTimeout(checkLoaded, 4000);
                                }
                            } catch (e) {
                                checkLoaded();
                            }
                        });
                        if (totalIframes === 0) resolve();
                    });
                }
            });
        } catch (e) {
            await sleep(1000);
        }

        try {
            const client = await page.target().createCDPSession();
            const movements = 2 + Math.floor(Math.random() * 3);
            for (let i = 0; i < movements; i++) {
                await client.send('Input.dispatchMouseEvent', {
                    type: 'mouseMoved',
                    x: Math.random() * viewport.width,
                    y: Math.random() * viewport.height
                });
                await sleep(200 + Math.random() * 300);
            }
        } catch (e) { }

        await page.evaluate(async () => {
            for (let i = 0; i < 3; i++) {
                window.scrollBy(0, 150 + Math.random() * 100);
                await new Promise(r => setTimeout(r, 200 + Math.random() * 150));
                if (Math.random() < 0.3) {
                    await new Promise(r => setTimeout(r, 300 + Math.random() * 200));
                }
            }
            window.scrollBy(0, -100);
            await new Promise(r => setTimeout(r, 250));
        });

        if (Math.random() < 0.2) {
            const randomPause = 500 + Math.random() * 1000;
            await sleep(randomPause);
        }

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

                const advertiserSelectors = ['.advertiser-name', '.advertiser-name-container', 'h1', '.creative-details-page-header-text', '.ad-details-heading'];

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

                        const bodyRect = document.body.getBoundingClientRect();
                        if (bodyRect.width < 50 || bodyRect.height < 50) {
                            return { ...data, isHidden: true };
                        }

                        const findPackageName = () => {
                            const packageRegex = /\b(com\.[a-zA-Z][a-zA-Z0-9_]*\.[a-zA-Z0-9_.]+)\b/g;
                            const foundPackages = new Set();

                            const allLinks = root.querySelectorAll('a[href]');
                            for (const link of allLinks) {
                                const href = link.href || '';
                                const idMatch = href.match(/[?&]id=([a-zA-Z][a-zA-Z0-9_.]+)/);
                                if (idMatch && idMatch[1] && idMatch[1].includes('.')) {
                                    foundPackages.add(idMatch[1]);
                                }
                                const matches = href.match(packageRegex);
                                if (matches) matches.forEach(m => foundPackages.add(m));
                            }

                            const allElements = root.querySelectorAll('*');
                            for (const el of allElements) {
                                for (const attr of el.attributes || []) {
                                    const val = attr.value || '';
                                    const matches = val.match(packageRegex);
                                    if (matches) matches.forEach(m => foundPackages.add(m));
                                    const idMatch = val.match(/[?&]id=([a-zA-Z][a-zA-Z0-9_.]+)/);
                                    if (idMatch && idMatch[1] && idMatch[1].includes('.')) {
                                        foundPackages.add(idMatch[1]);
                                    }
                                }
                            }

                            const scripts = root.querySelectorAll('script');
                            for (const script of scripts) {
                                const content = script.textContent || '';
                                const matches = content.match(packageRegex);
                                if (matches) matches.forEach(m => foundPackages.add(m));
                            }

                            const html = root.innerHTML || '';
                            const htmlMatches = html.match(packageRegex);
                            if (htmlMatches) htmlMatches.forEach(m => foundPackages.add(m));

                            const blacklistPatterns = ['com.google.android', 'com.android.', 'schema.org', 'w3.org'];
                            const validPackages = [...foundPackages].filter(pkg => {
                                if (pkg.length < 5 || pkg.length > 100) return false;
                                if (blacklistPatterns.some(bp => pkg.startsWith(bp))) return false;
                                if (pkg.split('.').length < 2) return false;
                                return true;
                            });

                            const comPackages = validPackages.filter(p => p.startsWith('com.'));
                            return comPackages[0] || validPackages[0] || null;
                        };

                        const buildPlayStoreUrl = (packageName) => {
                            if (!packageName) return null;
                            return `https://play.google.com/store/apps/details?id=${packageName}`;
                        };

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
                                    const patterns = [/[?&]adurl=([^&\s]+)/i, /[?&]dest=([^&\s]+)/i, /[?&]url=([^&\s]+)/i];
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

                                const pkgMatch = href.match(/[?&]id=([a-zA-Z][a-zA-Z0-9_.]+)/);
                                if (pkgMatch && pkgMatch[1] && pkgMatch[1].includes('.')) {
                                    return buildPlayStoreUrl(pkgMatch[1]);
                                }
                            } catch (e) { }

                            return null;
                        };

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
                            return clean;
                        };

                        const appNameDivSelectors = [
                            'div.KDwhZb-Gxk8ed-r4nke',
                            'div[class*="KDwhZb-Gxk8ed-r4nke"]',
                            'div[class*="KDwhZb"][class*="Gxk8ed"]',
                            'div.cS4Vcb-kb9wTc',
                            'div[class*="cS4Vcb-kb9wTc"]',
                            'div[class*="cS4Vcb-pGL6qe-c0XB9d"]'
                        ];

                        for (const selector of appNameDivSelectors) {
                            const elements = root.querySelectorAll(selector);
                            for (const el of elements) {
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

                                if (!data.appName) {
                                    const rawName = el.innerText || el.textContent || '';
                                    const appName = cleanAppName(rawName);
                                    if (appName && appName.toLowerCase() !== blacklist) {
                                        data.appName = appName;
                                    }
                                }

                                if (data.appName && storeLink) {
                                    return { appName: data.appName, storeLink, appSubtitle: data.appSubtitle, isVideo: true, isHidden: false };
                                } else if (storeLink && !data.storeLink) {
                                    data.storeLink = storeLink;
                                }
                            }
                        }

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

                        if (!data.storeLink) {
                            const packageName = findPackageName();
                            if (packageName) {
                                data.storeLink = buildPlayStoreUrl(packageName);
                            }
                        }

                        if (!data.appName) {
                            const textSels = ['div[class*="KDwhZb"] span', '[role="heading"]', 'div[class*="app-name"]', '.app-title'];
                            for (const sel of textSels) {
                                const elements = root.querySelectorAll(sel);
                                for (const el of elements) {
                                    const rawName = el.innerText || el.textContent || '';
                                    const appName = cleanAppName(rawName);
                                    if (appName && appName.toLowerCase() !== blacklist) {
                                        data.appName = appName;
                                        break;
                                    }
                                }
                                if (data.appName) break;
                            }
                        }

                        if (!data.appSubtitle) {
                            const subtitleSelectors = [
                                '.cS4Vcb-vnv8ic',
                                '[class*="cS4Vcb-vnv8ic"]',
                                'div.cS4Vcb-vnv8ic',
                                '[class*="vnv8ic"]',
                                '[data-asoch-targets*="Headline"]',
                                '[data-asoch-targets*="Description"]',
                                '.description',
                                'div[class*="description"]'
                            ];
                            for (const sel of subtitleSelectors) {
                                const els = root.querySelectorAll(sel);
                                for (const el of els) {
                                    let text = (el.innerText || el.textContent || '').trim();
                                    text = text.replace(/[\u200B-\u200D\uFEFF]/g, '');
                                    if (text && text.length > 2 && text.length < 150 && text !== data.appName) {
                                        const lower = text.toLowerCase();
                                        const blacklistSub = ['install', 'open', 'download', 'play', 'get', 'ad details', 'google play'];
                                        if (!blacklistSub.some(b => lower === b)) {
                                            data.appSubtitle = text;
                                            break;
                                        }
                                    }
                                }
                                if (data.appSubtitle) break;
                            }
                        }

                        data.isHidden = false;
                        return data;
                    }, blacklistName);

                    if (frameData.isHidden) continue;

                    if (frameData.appSubtitle && result.appSubtitle === 'NOT_FOUND') {
                        result.appSubtitle = frameData.appSubtitle;
                    }

                    if (frameData.appName && frameData.storeLink && result.appName === 'NOT_FOUND') {
                        result.appName = cleanName(frameData.appName);
                        result.storeLink = frameData.storeLink;
                        console.log(`  ✓ Found: ${result.appName} -> ${result.storeLink.substring(0, 60)}...`);
                        if (result.appSubtitle !== 'NOT_FOUND') {
                            console.log(`  ✓ Subtitle: ${result.appSubtitle}`);
                        }
                        break;
                    }

                    if (frameData.appName && !frameData.storeLink && result.appName === 'NOT_FOUND') {
                        result.appName = cleanName(frameData.appName);
                    }
                } catch (e) { }
            }

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

        const success = data.appName !== 'NOT_FOUND' || data.storeLink !== 'NOT_FOUND';

        if (success) {
            return data;
        } else {
            console.log(`  ⚠️ Attempt ${attempt} - no data found. Retrying...`);
        }

        await randomDelay(2000, 4000);
    }
    return { advertiserName: 'NOT_FOUND', storeLink: 'NOT_FOUND', appName: 'NOT_FOUND', appSubtitle: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting App Name Extraction Agent (TOP TO BOTTOM)...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Columns: A=Advertiser, B=URL, C=App Link, D=App Name, F=Subtitle\n`);

    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000;

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ All rows complete. Nothing to process.');
        process.exit(0);
    }

    const needsMeta = toProcess.filter(x => x.needsMetadata).length;
    console.log(`📊 Found ${toProcess.length} rows to process (from top to bottom)\n`);

    console.log(PROXIES.length ? `🔁 Proxy rotation enabled (${PROXIES.length} proxies)` : '🔁 Running direct');

    const PAGES_PER_BROWSER = 30;
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
        consecutiveSuccessBatches = 0;

        while (sessionProcessed < currentSessionSize && !blocked) {
            const batchSize = Math.min(CONCURRENT_PAGES, currentSessionSize - sessionProcessed);
            const batch = toProcess.slice(currentIndex, currentIndex + batchSize);

            console.log(`📦 Batch ${currentIndex + 1}-${currentIndex + batchSize} / ${toProcess.length}`);

            try {
                const results = await Promise.all(batch.map(async (item, index) => {
                    if (index > 0) {
                        const staggerDelay = PAGE_LOAD_DELAY_MIN + Math.random() * (PAGE_LOAD_DELAY_MAX - PAGE_LOAD_DELAY_MIN);
                        await sleep(staggerDelay * index);
                    }
                    const data = await extractWithRetry(item, browser);
                    return {
                        rowIndex: item.rowIndex,
                        advertiserName: data.advertiserName,
                        storeLink: data.storeLink,
                        appName: data.appName,
                        appSubtitle: data.appSubtitle
                    };
                }));

                results.forEach(r => {
                    console.log(`  → Row ${r.rowIndex + 1}: Name=${r.appName} | Subtitle=${r.appSubtitle?.substring(0, 30) || 'NOT_FOUND'}... | Link=${r.storeLink?.substring(0, 40) || 'NOT_FOUND'}`);
                });

                const successfulResults = results.filter(r => r.storeLink !== 'BLOCKED' && r.appName !== 'BLOCKED');
                const blockedResults = results.filter(r => r.storeLink === 'BLOCKED' || r.appName === 'BLOCKED');

                if (results.length > 0) {
                    await batchWriteToSheet(sheets, results);
                    console.log(`  ✅ Wrote ${results.length} results to sheet (${successfulResults.length} successful, ${blockedResults.length} blocked)`);
                    await sleep(1000 + Math.random() * 1000);
                }

                if ((currentIndex / batchSize) % 10 === 0) {
                    const elapsed = Math.floor((Date.now() - sessionStartTime) / 60000);
                    const remaining = toProcess.length - currentIndex;
                    console.log(`\n📊 PROGRESS: ${currentIndex}/${toProcess.length} processed (${remaining} remaining) | Runtime: ${elapsed} mins\n`);
                }

                if (blockedResults.length > 0) {
                    console.log(`  🛑 Block detected (${blockedResults.length} blocked, ${successfulResults.length} successful). Closing browser and rotating...`);
                    proxyStats.totalBlocks++;
                    proxyStats.perProxy[proxy || 'DIRECT'] = (proxyStats.perProxy[proxy || 'DIRECT'] || 0) + 1;
                    blocked = true;
                    consecutiveSuccessBatches = 0;
                } else {
                    consecutiveSuccessBatches++;
                }

                currentIndex += batchSize;
                sessionProcessed += batchSize;
            } catch (err) {
                console.error(`  ❌ Batch error: ${err.message}`);
                currentIndex += batchSize;
                sessionProcessed += batchSize;
            }

            if (!blocked) {
                const adaptiveMultiplier = Math.max(0.7, 1 - (consecutiveSuccessBatches * 0.05));
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
