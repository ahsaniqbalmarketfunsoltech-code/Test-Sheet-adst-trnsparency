/**
 * UNIFIED GOOGLE ADS TRANSPARENCY AGENT
 * =====================================
 * Extracts ONLY: App Name, App Link (from meta tag), App Headline/Subtitle
 * 
 * Sheet Structure:
 *   Column B: Ads URL
 *   Column C: App Link (from meta[data-asoch-meta])
 *   Column D: App Name
 *   Column F: App Headline/Subtitle (cS4Vcb-vnv8ic)
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
const SHEET_NAME = process.env.SHEET_NAME || 'Test data'; // Can be overridden via env var
const CREDENTIALS_PATH = './credentials.json';
const SHEET_BATCH_SIZE = parseInt(process.env.SHEET_BATCH_SIZE) || 10000; // Rows to load per batch
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 7; // Increased for speed
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 3; // Reduced retries for speed
const RETRY_WAIT_MULTIPLIER = 1.2; // Slightly reduced
const PAGE_LOAD_DELAY_MIN = parseInt(process.env.PAGE_LOAD_DELAY_MIN) || 500; // Faster staggered starts
const PAGE_LOAD_DELAY_MAX = parseInt(process.env.PAGE_LOAD_DELAY_MAX) || 1500; // Faster staggered starts

const BATCH_DELAY_MIN = parseInt(process.env.BATCH_DELAY_MIN) || 3000; // Faster batch processing
const BATCH_DELAY_MAX = parseInt(process.env.BATCH_DELAY_MAX) || 6000; // Faster batch processing

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
    let credentials;

    // Priority 1: Environment Variable (most secure for CI)
    if (process.env.GOOGLE_CREDENTIALS) {
        try {
            credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
        } catch (err) {
            console.error('❌ Error parsing GOOGLE_CREDENTIALS environment variable.');
            console.error('   Make sure the secret is a valid JSON string.');
            throw err;
        }
    }
    // Priority 2: Credentials File
    else if (fs.existsSync(CREDENTIALS_PATH)) {
        try {
            const content = fs.readFileSync(CREDENTIALS_PATH, 'utf8');
            if (content.trim().startsWith('***')) {
                throw new Error('Credentials file contains masked secret (***). This usually happens when the secret is piped incorrectly in CI.');
            }
            credentials = JSON.parse(content);
        } catch (err) {
            console.error(`❌ Error parsing ${CREDENTIALS_PATH}`);
            console.error(`   Error: ${err.message}`);
            if (err.message.includes('JSON')) {
                const content = fs.readFileSync(CREDENTIALS_PATH, 'utf8');
                console.error(`   File starts with: "${content.substring(0, 20)}..."`);
            }
            throw err;
        }
    } else {
        throw new Error(`Google credentials not found. Set GOOGLE_CREDENTIALS env var or create ${CREDENTIALS_PATH}`);
    }

    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const authClient = await auth.getClient();
    return google.sheets({ version: 'v4', auth: authClient });
}

async function getUrlData(sheets, batchSize = SHEET_BATCH_SIZE) {
    const toProcess = [];
    let startRow = 1; // Start from row 2 (skip header)
    let hasMoreData = true;
    let totalProcessed = 0;

    console.log(`📊 Loading data in batches of ${batchSize} rows...`);

    while (hasMoreData) {
        try {
            const endRow = startRow + batchSize - 1;
            const range = `${SHEET_NAME}!A${startRow + 1}:G${endRow + 1}`; // A-G columns

            const response = await sheets.spreadsheets.values.get({
                spreadsheetId: SPREADSHEET_ID,
                range: range,
            });

            const rows = response.data.values || [];

            if (rows.length === 0) {
                hasMoreData = false;
                break;
            }

            for (let i = 0; i < rows.length; i++) {
                const row = rows[i];
                const actualRowIndex = startRow + i; // Actual row number in sheet
                const url = row[1]?.trim() || '';
                const storeLink = row[2]?.trim() || '';
                const appName = row[3]?.trim() || '';
                const appSubtitle = row[5]?.trim() || '';

                if (!url) continue;

                // NEW CRITERIA: ONLY process if Column C (storeLink) is EMPTY
                if (storeLink && storeLink.trim() !== '') {
                    continue; // Skip - already has data in Column C
                }

                // Process rows that need extraction
                const needsMetadata = true; // Since it is empty, it needs metadata
                toProcess.push({
                    url,
                    rowIndex: actualRowIndex,
                    needsMetadata,
                    existingStoreLink: storeLink,
                    existingAppName: appName,
                    existingAppSubtitle: appSubtitle
                });
            }

            totalProcessed += rows.length;
            console.log(`  ✓ Processed ${totalProcessed} rows, found ${toProcess.length} to process`);

            // If we got less than batchSize rows, we've reached the end
            if (rows.length < batchSize) {
                hasMoreData = false;
            } else {
                startRow = endRow + 1;
                // Small delay between batches to avoid rate limits
                await sleep(100);
            }
        } catch (error) {
            console.error(`  ⚠️ Error loading batch starting at row ${startRow}: ${error.message}`);
            // If error, try to continue with next batch
            startRow += batchSize;
            await sleep(500); // Wait a bit longer on error
        }
    }

    console.log(`📊 Total: ${totalProcessed} rows scanned, ${toProcess.length} rows need extraction\n`);
    return toProcess;
}

async function batchWriteToSheet(sheets, updates) {
    if (updates.length === 0) return;

    const data = [];
    // Write ONLY: App Link (C), App Name (D), App Headline (F)
    updates.forEach(({ rowIndex, storeLink, appName, appSubtitle }) => {
        const rowNum = rowIndex + 1;

        // Write store link (Column C) - ALWAYS write
        const storeLinkValue = storeLink && storeLink !== 'SKIP' ? storeLink : 'NOT_FOUND';
        data.push({ range: `${SHEET_NAME}!C${rowNum}`, values: [[storeLinkValue]] });

        // Write app name (Column D) - ALWAYS write
        const appNameValue = appName && appName !== 'SKIP' ? appName : 'NOT_FOUND';
        data.push({ range: `${SHEET_NAME}!D${rowNum}`, values: [[appNameValue]] });

        // Write app headline/subtitle (Column F) - ALWAYS write
        const appSubtitleValue = appSubtitle || 'NOT_FOUND';
        data.push({ range: `${SHEET_NAME}!F${rowNum}`, values: [[appSubtitleValue]] });
    });

    if (data.length === 0) return;

    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: { valueInputOption: 'RAW', data: data }
        });
        console.log(`  ✅ Wrote ${updates.length} results to sheet`);
    } catch (error) {
        console.error(`  ❌ Write error:`, error.message);
    }
}

// ============================================
// UNIFIED EXTRACTION - ONE VISIT PER URL
// Metadata extracted on same page
// ============================================
async function extractAllInOneVisit(url, browser, needsMetadata, existingStoreLink, attempt = 1) {
    const page = await browser.newPage();
    let result = {
        appName: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        storeLink: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        appSubtitle: needsMetadata ? 'NOT_FOUND' : 'SKIP'
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

        // Reject if looks like CSS or generic text
        const lowerClean = cleaned.toLowerCase();
        if (lowerClean === 'ad details' || lowerClean === 'google ads' || lowerClean === 'sponsored' || lowerClean === 'advertisement') {
            return 'NOT_FOUND';
        }

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
        // Abort more resource types for speed: image, font, stylesheet (optional but fast), and tracking
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
            return { appName: 'BLOCKED', storeLink: 'BLOCKED', appSubtitle: 'BLOCKED' };
        }

        // Wait for dynamic elements to settle (optimized for speed)
        const baseWait = 2000 + Math.random() * 1000; // Reduced: 2000-3000ms for faster processing
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
                                setTimeout(resolve, 800); // Reduced: faster iframe loading
                            }
                        };
                        iframes.forEach(iframe => {
                            try {
                                if (iframe.contentDocument && iframe.contentDocument.readyState === 'complete') {
                                    checkLoaded();
                                } else {
                                    iframe.onload = checkLoaded;
                                    // Timeout after 2 seconds per iframe (reduced for speed)
                                    setTimeout(checkLoaded, 2000);
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
            // If iframe check fails, wait a bit anyway (reduced for speed)
            await sleep(500); // Reduced from 1000ms
        }

        // Reduced mouse movements for speed (still human-like but faster)
        try {
            const client = await page.target().createCDPSession();
            const movements = 1 + Math.floor(Math.random() * 2); // 1-2 movements (reduced)
            for (let i = 0; i < movements; i++) {
                await client.send('Input.dispatchMouseEvent', {
                    type: 'mouseMoved',
                    x: Math.random() * viewport.width,
                    y: Math.random() * viewport.height
                });
                await sleep(100 + Math.random() * 150); // Faster movements
            }
        } catch (e) { /* Ignore if CDP fails */ }

        // All ads (video, text, image) will now be processed.

        // Faster human-like interaction (optimized for speed)
        await page.evaluate(async () => {
            // Reduced scrolling iterations for speed
            for (let i = 0; i < 2; i++) { // Reduced from 3 to 2
                window.scrollBy(0, 200 + Math.random() * 100);
                await new Promise(r => setTimeout(r, 150 + Math.random() * 100)); // Faster
            }
            // Scroll back up a bit
            window.scrollBy(0, -100);
            await new Promise(r => setTimeout(r, 150)); // Faster
        });

        // Reduced random pause (10% chance instead of 20%)
        if (Math.random() < 0.1) {
            const randomPause = 300 + Math.random() * 500; // Reduced pause time
            await sleep(randomPause);
        }

        // =====================================================
        // EXTRACT: App Name, App Link (from meta tag), App Headline
        // =====================================================
        if (needsMetadata) {
            console.log(`  📊 Extracting: App Name, App Link, App Headline...`);

            // Get blacklist words for filtering app names
            const blacklistName = 'ad details';

            const frames = page.frames();
            for (const frame of frames) {
                try {
                    const frameData = await frame.evaluate((blacklist) => {
                        const data = { appName: null, storeLink: null, appSubtitle: null, metaStoreLinkFound: null, metaTagCount: 0 };
                        const root = document.querySelector('#portrait-landscape-phone') || document.body;

                        // Check if this frame content is visible (has dimensions)
                        const bodyRect = document.body.getBoundingClientRect();
                        if (bodyRect.width < 50 || bodyRect.height < 50) {
                            return { ...data, isHidden: true };
                        }

                        // =====================================================
                        // EXTRACT STORE LINK FROM FIRST META DATA-ASOCH-META
                        // Get the FIRST meta tag with data-asoch-meta and extract package name
                        // =====================================================
                        const extractFromMetaTag = () => {
                            try {
                                // Get the FIRST meta tag with data-asoch-meta (as user specified)
                                const firstMetaTag = root.querySelector('meta[data-asoch-meta]');
                                if (!firstMetaTag) return null;

                                const metaData = firstMetaTag.getAttribute('data-asoch-meta');
                                if (!metaData) return null;

                                // Debug: count all meta tags found
                                const allMetaTags = root.querySelectorAll('meta[data-asoch-meta]');
                                data.metaTagCount = allMetaTags.length;

                                // Extract package name directly from the FIRST meta tag
                                // Try multiple patterns to find the package name

                                // Pattern 1: Direct regex extraction (fastest)
                                const fastPackageMatch = metaData.match(/id%3D([a-zA-Z0-9._]+)|[?&]id=([a-zA-Z0-9._]+)/);
                                if (fastPackageMatch) {
                                    const packageName = fastPackageMatch[1] || fastPackageMatch[2];
                                    if (packageName && packageName.length > 3) {
                                        return `https://play.google.com/store/apps/details?id=${packageName}`;
                                    }
                                }

                                // Pattern 2: Parse JSON and extract from ad0 entry
                                try {
                                    const parsed = JSON.parse(metaData);
                                    if (Array.isArray(parsed) && parsed.length > 0) {
                                        const firstArray = parsed[0];
                                        if (Array.isArray(firstArray)) {
                                            // Find "ad0" entry
                                            for (const entry of firstArray) {
                                                if (Array.isArray(entry) && entry.length > 1 && entry[0] === 'ad0') {
                                                    const urlString = entry[1];
                                                    if (urlString) {
                                                        // Extract from adurl parameter
                                                        const adurlMatch = urlString.match(/[?&]adurl=([^&\s]+)/i);
                                                        if (adurlMatch) {
                                                            try {
                                                                const decodedUrl = decodeURIComponent(adurlMatch[1]);
                                                                if (decodedUrl.includes('play.google.com/store/apps/details')) {
                                                                    const pkgMatch = decodedUrl.match(/[?&]id=([a-zA-Z0-9._]+)/);
                                                                    if (pkgMatch && pkgMatch[1]) {
                                                                        return `https://play.google.com/store/apps/details?id=${pkgMatch[1]}`;
                                                                    }
                                                                }
                                                            } catch (e) { }
                                                        }

                                                        // Extract package directly from URL string
                                                        const pkgMatch = urlString.match(/id%3D([a-zA-Z0-9._]+)|[?&]id=([a-zA-Z0-9._]+)/);
                                                        if (pkgMatch) {
                                                            const pkgName = pkgMatch[1] || pkgMatch[2];
                                                            if (pkgName) {
                                                                return `https://play.google.com/store/apps/details?id=${pkgName}`;
                                                            }
                                                        }
                                                    }
                                                    break; // Found ad0, stop
                                                }
                                            }
                                        }
                                    }
                                } catch (e) {
                                    // JSON parse failed, already tried fast path above
                                }
                            } catch (e) {
                                // Silently fail
                            }
                            return null;
                        };

                        // =====================================================
                        // EXTRACT STORE LINK - ONLY FROM FIRST META DATA-ASOCH-META
                        // This is the ONLY method to extract store links
                        // =====================================================
                        const metaStoreLink = extractFromMetaTag();
                        if (metaStoreLink) {
                            data.storeLink = metaStoreLink;
                            data.metaStoreLinkFound = metaStoreLink; // Flag to log outside
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

                            // BLACKLIST CHECK: Avoid generic text
                            const lowerClean = clean.toLowerCase();
                            if (lowerClean === 'ad details' || lowerClean === 'google ads' || lowerClean === 'sponsored' || lowerClean === 'advertisement') {
                                return null;
                            }

                            if (clean.length < 2 || clean.length > 80) return null;
                            if (/^[\d\s\W]+$/.test(clean)) return null;
                            return clean;
                        };

                        // =====================================================
                        // EXTRACT APP NAME ONLY (no store link extraction here)
                        // =====================================================
                        const appNameSelectors = [
                            'a[data-asoch-targets*="ochAppName"]',
                            'a[data-asoch-targets*="appname" i]',
                            'a[data-asoch-targets*="rrappname" i]',
                            'a[class*="short-app-name"]',
                            '.short-app-name a'
                        ];

                        for (const selector of appNameSelectors) {
                            const elements = root.querySelectorAll(selector);
                            for (const el of elements) {
                                const rawName = el.innerText || el.textContent || '';
                                const appName = cleanAppName(rawName);
                                if (!appName || appName.toLowerCase() === blacklist) continue;

                                // Only extract app name, NOT store link (store link comes from meta tag only)
                                if (appName && !data.appName) {
                                    data.appName = appName;
                                    break;
                                }
                            }
                            if (data.appName) break;
                        }

                        // Fallback for app name only
                        if (!data.appName) {
                            const textSels = ['[role="heading"]', 'div[class*="app-name"]', '.app-title'];
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

                        data.isHidden = false;
                        return data;
                    }, blacklistName);

                    // Subtitle extraction will be done separately with hover

                    // Extract Image URL if found

                    // Log meta tag extraction results
                    if (frameData.metaTagCount) {
                        console.log(`  🔍 Found ${frameData.metaTagCount} meta tag(s) in frame`);
                    }
                    if (frameData.metaStoreLinkFound) {
                        console.log(`  🔗 Found store link from meta tag: ${frameData.metaStoreLinkFound.substring(0, 60)}...`);
                    }

                    // Skip hidden frames
                    if (frameData.isHidden) continue;

                    // DETECT TEXT AD: Check if we have BOTH app name AND subtitle/headline
                    const foundAppName = frameData.appName && result.appName === 'NOT_FOUND';
                    const foundSubtitle = frameData.appSubtitle && result.appSubtitle === 'NOT_FOUND';
                    const hasAppName = foundAppName || (result.appName && result.appName !== 'NOT_FOUND' && result.appName !== 'Ad Details');
                    const hasSubtitle = foundSubtitle || (result.appSubtitle && result.appSubtitle !== 'NOT_FOUND');
                    const isTextAd = hasAppName && hasSubtitle;

                    // If TEXT AD detected: Focus on store link extraction, skip other work
                    if (isTextAd) {
                        console.log(`  📝 TEXT AD DETECTED: Found name + subtitle - focusing on store link extraction`);

                        // Store the name and subtitle if found
                        if (foundAppName) {
                            result.appName = cleanName(frameData.appName);
                        }
                        if (foundSubtitle) {
                            result.appSubtitle = frameData.appSubtitle;
                        }

                        // PRIORITY: Extract store link for text ads
                        if (frameData.storeLink && result.storeLink === 'NOT_FOUND') {
                            result.storeLink = frameData.storeLink;
                            console.log(`  ✓ TEXT AD COMPLETE: ${result.appName} -> ${result.storeLink.substring(0, 60)}...`);
                            break; // Text ad complete, stop searching
                        }

                        // Continue looking for store link in other frames
                        continue;
                    }

                    // For non-text ads, continue normal extraction
                    // If we found store link, use it
                    if (frameData.storeLink && result.storeLink === 'NOT_FOUND') {
                        result.storeLink = frameData.storeLink;
                        console.log(`  🔗 Found store link: ${result.storeLink.substring(0, 60)}...`);
                    }

                    // If we found BOTH app name AND store link, use this immediately
                    if (frameData.appName && frameData.storeLink && result.appName === 'NOT_FOUND') {
                        result.appName = cleanName(frameData.appName);
                        result.storeLink = frameData.storeLink;
                        console.log(`  ✓ Found: ${result.appName} -> ${result.storeLink.substring(0, 60)}...`);
                        break; // We have both, stop searching
                    }

                    // If we only found name (no link), store it but keep looking
                    if (frameData.appName && !frameData.storeLink && result.appName === 'NOT_FOUND') {
                        result.appName = cleanName(frameData.appName);
                        // DON'T break - continue looking
                    }

                    // If we only found store link (no name), keep it but continue looking for name
                    if (frameData.storeLink && !frameData.appName && result.storeLink === 'NOT_FOUND') {
                        result.storeLink = frameData.storeLink;
                        // Continue looking for app name
                    }
                } catch (e) { }
            }

            // Extract store link from meta tags on main page (if not found in iframes) - OPTIMIZED
            if (needsMetadata && result.storeLink === 'NOT_FOUND') {
                try {
                    const metaStoreLink = await page.evaluate(() => {
                        try {
                            // Get the FIRST meta tag with data-asoch-meta (as user specified)
                            const firstMetaTag = document.querySelector('meta[data-asoch-meta]');
                            if (!firstMetaTag) return null;

                            const metaData = firstMetaTag.getAttribute('data-asoch-meta');
                            if (!metaData) return null;

                            // Extract package name directly from the FIRST meta tag
                            // Pattern 1: Direct regex extraction (fastest)
                            const fastPackageMatch = metaData.match(/id%3D([a-zA-Z0-9._]+)|[?&]id=([a-zA-Z0-9._]+)/);
                            if (fastPackageMatch) {
                                const packageName = fastPackageMatch[1] || fastPackageMatch[2];
                                if (packageName && packageName.length > 3) {
                                    return `https://play.google.com/store/apps/details?id=${packageName}`;
                                }
                            }

                            // Pattern 2: Parse JSON and extract from ad0 entry
                            try {
                                const parsed = JSON.parse(metaData);
                                if (Array.isArray(parsed) && parsed.length > 0) {
                                    const firstArray = parsed[0];
                                    if (Array.isArray(firstArray)) {
                                        // Find "ad0" entry
                                        for (const entry of firstArray) {
                                            if (Array.isArray(entry) && entry.length > 1 && entry[0] === 'ad0') {
                                                const urlString = entry[1];
                                                if (urlString) {
                                                    // Extract from adurl parameter
                                                    const adurlMatch = urlString.match(/[?&]adurl=([^&\s]+)/i);
                                                    if (adurlMatch) {
                                                        try {
                                                            const decodedUrl = decodeURIComponent(adurlMatch[1]);
                                                            if (decodedUrl.includes('play.google.com/store/apps/details')) {
                                                                const pkgMatch = decodedUrl.match(/[?&]id=([a-zA-Z0-9._]+)/);
                                                                if (pkgMatch && pkgMatch[1]) {
                                                                    return `https://play.google.com/store/apps/details?id=${pkgMatch[1]}`;
                                                                }
                                                            }
                                                        } catch (e) { }
                                                    }

                                                    // Extract package directly from URL string
                                                    const pkgMatch = urlString.match(/id%3D([a-zA-Z0-9._]+)|[?&]id=([a-zA-Z0-9._]+)/);
                                                    if (pkgMatch) {
                                                        const pkgName = pkgMatch[1] || pkgMatch[2];
                                                        if (pkgName) {
                                                            return `https://play.google.com/store/apps/details?id=${pkgName}`;
                                                        }
                                                    }
                                                }
                                                break; // Found ad0, stop
                                            }
                                        }
                                    }
                                }
                            } catch (e) {
                                // JSON parse failed, already tried fast path above
                            }
                        } catch (e) {
                            // Silently fail
                        }
                        return null;
                    });

                    if (metaStoreLink) {
                        result.storeLink = metaStoreLink;
                        console.log(`  ✓ Found store link from meta tag: ${result.storeLink.substring(0, 60)}...`);
                    }
                } catch (e) {
                    // Silently fail and continue
                }
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

            // =====================================================
            // EXTRACT SUBTITLE - Only cS4Vcb-vnv8ic class with hover
            // Non-blocking: If this fails, other extractions continue
            // =====================================================
            // Only extract subtitle if we have app name (text ad detection)
            const isTextAdForSubtitle = result.appName && result.appName !== 'NOT_FOUND' && result.appName !== 'Ad Details';

            if (needsMetadata && result.appSubtitle === 'NOT_FOUND' && isTextAdForSubtitle) {
                // Run subtitle extraction in a separate try-catch to ensure it doesn't block other extractions
                try {
                    console.log(`  📝 Extracting subtitle for text ad (cS4Vcb-vnv8ic)...`);

                    // Reduced wait for dynamic content (optimized for speed)
                    await sleep(500 + Math.random() * 300); // Reduced from 1000-1500ms to 500-800ms

                    const client = await page.target().createCDPSession();

                    // First, try extracting from iframes (most common location for ad creatives)
                    const frames = page.frames();
                    console.log(`  🔍 Checking ${frames.length} iframes for subtitle...`);

                    for (const frame of frames) {
                        try {
                            // Find subtitle elements in this frame - try multiple selectors
                            const subtitleInfo = await frame.evaluate(() => {
                                const root = document.querySelector('#portrait-landscape-phone') || document.body;

                                // Try exact class first
                                let subtitleEls = root.querySelectorAll('.cS4Vcb-vnv8ic');

                                // If not found, try partial match
                                if (subtitleEls.length === 0) {
                                    subtitleEls = root.querySelectorAll('[class*="cS4Vcb"][class*="vnv8ic"]');
                                }

                                // If still not found, try any element with vnv8ic
                                if (subtitleEls.length === 0) {
                                    subtitleEls = root.querySelectorAll('[class*="vnv8ic"]');
                                }

                                const elements = [];

                                for (const el of subtitleEls) {
                                    const rect = el.getBoundingClientRect();
                                    if (rect.width > 0 && rect.height > 0) {
                                        const text = (el.innerText || el.textContent || '').trim();
                                        if (text && text.length >= 3 && text.length <= 200) {
                                            // Less strict validation - just avoid CSS-like content
                                            if (!text.includes('{') && !text.includes('px') && !text.includes('height') && !text.includes('width')) {
                                                // Check if this is in the ad creative area (not header/footer)
                                                const bodyRect = document.body.getBoundingClientRect();
                                                // Element should be in the visible/middle area
                                                if (rect.top > bodyRect.top + 30 && rect.bottom < bodyRect.bottom - 30) {
                                                    elements.push({
                                                        x: rect.left + rect.width / 2,
                                                        y: rect.top + rect.height / 2,
                                                        text: text,
                                                        area: rect.width * rect.height
                                                    });
                                                }
                                            }
                                        }
                                    }
                                }

                                // Sort by area (larger elements are more likely to be the main subtitle)
                                elements.sort((a, b) => b.area - a.area);

                                return elements;
                            });

                            console.log(`  🔍 Frame found ${subtitleInfo ? subtitleInfo.length : 0} subtitle candidates`);

                            // Hover on each subtitle element found in this frame
                            if (subtitleInfo && subtitleInfo.length > 0) {
                                for (const subtitleEl of subtitleInfo) {
                                    try {
                                        // Get iframe position to calculate absolute coordinates
                                        const iframe = await frame.frameElement();
                                        if (iframe) {
                                            const iframeRect = await iframe.boundingBox();
                                            if (iframeRect) {
                                                const absoluteX = iframeRect.x + subtitleEl.x;
                                                const absoluteY = iframeRect.y + subtitleEl.y;

                                                // Scroll to element first to ensure it's visible
                                                await page.evaluate((x, y) => {
                                                    window.scrollTo(x - window.innerWidth / 2, y - window.innerHeight / 2);
                                                }, absoluteX, absoluteY);
                                                await sleep(200); // Reduced from 300ms

                                                // Hover on the subtitle element
                                                await client.send('Input.dispatchMouseEvent', {
                                                    type: 'mouseMoved',
                                                    x: absoluteX,
                                                    y: absoluteY
                                                });
                                                await sleep(400 + Math.random() * 200); // Reduced: 400-600ms (was 800-1200ms)

                                                // After hover, extract the text from the subtitle element
                                                const hoveredText = await frame.evaluate(() => {
                                                    const root = document.querySelector('#portrait-landscape-phone') || document.body;

                                                    // Try exact class first
                                                    let subtitleEls = root.querySelectorAll('.cS4Vcb-vnv8ic');

                                                    // If not found, try partial match
                                                    if (subtitleEls.length === 0) {
                                                        subtitleEls = root.querySelectorAll('[class*="cS4Vcb"][class*="vnv8ic"]');
                                                    }

                                                    // If still not found, try any element with vnv8ic
                                                    if (subtitleEls.length === 0) {
                                                        subtitleEls = root.querySelectorAll('[class*="vnv8ic"]');
                                                    }

                                                    const candidates = [];

                                                    // Return first valid subtitle in ad creative area (after hover, this should be the correct one)
                                                    for (const el of subtitleEls) {
                                                        const rect = el.getBoundingClientRect();
                                                        if (rect.width > 0 && rect.height > 0) {
                                                            const bodyRect = document.body.getBoundingClientRect();
                                                            // Element should be in the visible/middle area (ad creative)
                                                            if (rect.top > bodyRect.top + 30 && rect.bottom < bodyRect.bottom - 30) {
                                                                const text = (el.innerText || el.textContent || '').trim();
                                                                if (text && text.length >= 3 && text.length <= 200) {
                                                                    if (!text.includes('{') && !text.includes('px') && !text.includes('height') && !text.includes('width')) {
                                                                        candidates.push({ text, area: rect.width * rect.height });
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }

                                                    // Return the largest element (most likely the main subtitle)
                                                    if (candidates.length > 0) {
                                                        candidates.sort((a, b) => b.area - a.area);
                                                        return candidates[0].text;
                                                    }

                                                    return null;
                                                });

                                                if (hoveredText) {
                                                    result.appSubtitle = hoveredText;
                                                    console.log(`  ✓ Found subtitle: ${result.appSubtitle.substring(0, 50)}...`);
                                                    break; // Found it, stop searching
                                                }
                                            }
                                        }
                                    } catch (e) {
                                        // Continue to next element - don't let one error stop the process
                                        console.log(`  ⚠️ Hover error (continuing): ${e.message}`);
                                    }
                                }

                                if (result.appSubtitle !== 'NOT_FOUND') break; // Found subtitle, stop checking frames
                            }
                        } catch (e) {
                            // Cross-origin iframe or error, skip silently and continue
                            console.log(`  ⚠️ Frame error (continuing): ${e.message}`);
                        }
                    }

                    // Fallback: try main page if not found in iframes
                    if (result.appSubtitle === 'NOT_FOUND') {
                        console.log(`  🔍 Checking main page for subtitle...`);
                        const subtitleElements = await page.evaluate(() => {
                            const elements = [];

                            // Try exact class first
                            let allElements = document.querySelectorAll('.cS4Vcb-vnv8ic');

                            // If not found, try partial match
                            if (allElements.length === 0) {
                                allElements = document.querySelectorAll('[class*="cS4Vcb"][class*="vnv8ic"]');
                            }

                            // If still not found, try any element with vnv8ic
                            if (allElements.length === 0) {
                                allElements = document.querySelectorAll('[class*="vnv8ic"]');
                            }

                            allElements.forEach(el => {
                                const rect = el.getBoundingClientRect();
                                if (rect.width > 0 && rect.height > 0) {
                                    const text = (el.innerText || el.textContent || '').trim();
                                    if (text && text.length >= 3 && text.length <= 200) {
                                        if (!text.includes('{') && !text.includes('px') && !text.includes('height') && !text.includes('width')) {
                                            const bodyRect = document.body.getBoundingClientRect();
                                            if (rect.top > bodyRect.top + 50 && rect.bottom < bodyRect.bottom - 50) {
                                                elements.push({
                                                    x: rect.left + rect.width / 2,
                                                    y: rect.top + rect.height / 2,
                                                    text: text,
                                                    area: rect.width * rect.height
                                                });
                                            }
                                        }
                                    }
                                }
                            });

                            // Sort by area (larger elements first)
                            elements.sort((a, b) => b.area - a.area);

                            return elements;
                        });

                        console.log(`  🔍 Main page found ${subtitleElements.length} subtitle candidates`);

                        // Hover on main page subtitle elements
                        for (const subtitleEl of subtitleElements) {
                            try {
                                // Scroll to element first
                                await page.evaluate((x, y) => {
                                    window.scrollTo(x - window.innerWidth / 2, y - window.innerHeight / 2);
                                }, subtitleEl.x, subtitleEl.y);
                                await sleep(200); // Reduced from 300ms

                                await client.send('Input.dispatchMouseEvent', {
                                    type: 'mouseMoved',
                                    x: subtitleEl.x,
                                    y: subtitleEl.y
                                });
                                await sleep(400 + Math.random() * 200); // Reduced: 400-600ms (was 800-1200ms)

                                const hoveredText = await page.evaluate((x, y) => {
                                    const element = document.elementFromPoint(x, y);
                                    if (element) {
                                        // Check if element or its parent has the class
                                        let checkEl = element;
                                        for (let i = 0; i < 3 && checkEl; i++) {
                                            if (checkEl.classList && (checkEl.classList.contains('cS4Vcb-vnv8ic') ||
                                                checkEl.className.includes('vnv8ic'))) {
                                                const text = (checkEl.innerText || checkEl.textContent || '').trim();
                                                if (text && text.length >= 3 && text.length <= 200) {
                                                    if (!text.includes('{') && !text.includes('px')) {
                                                        return text;
                                                    }
                                                }
                                            }
                                            checkEl = checkEl.parentElement;
                                        }
                                    }
                                    return null;
                                }, subtitleEl.x, subtitleEl.y);

                                if (hoveredText) {
                                    result.appSubtitle = hoveredText;
                                    console.log(`  ✓ Found subtitle on main page: ${result.appSubtitle.substring(0, 50)}...`);
                                    break;
                                }
                            } catch (e) {
                                // Continue to next element - don't let one error stop the process
                                console.log(`  ⚠️ Main page hover error (continuing): ${e.message}`);
                            }
                        }
                    }

                    if (result.appSubtitle === 'NOT_FOUND') {
                        console.log(`  ⚠️ Subtitle not found - continuing with other extractions`);
                    }
                } catch (e) {
                    // Subtitle extraction failed, but continue with other fields
                    console.log(`  ⚠️ Subtitle extraction failed (continuing): ${e.message}`);
                    result.appSubtitle = 'NOT_FOUND'; // Ensure it's set to NOT_FOUND
                }
            }
        }

        await page.close();
        return result;
    } catch (err) {
        console.error(`  ❌ Critical error: ${err.message}`);
        // Return whatever we successfully extracted, not all ERROR
        // This ensures other fields are still written even if extraction fails
        try {
            await page.close();
        } catch (e) {
            // Ignore close errors
        }
        // Return partial results - only set ERROR for fields that weren't extracted
        return {
            appName: result.appName || 'ERROR',
            storeLink: result.storeLink || 'ERROR',
            appSubtitle: result.appSubtitle || 'ERROR'
        };
    }
}

async function extractWithRetry(item, browser) {
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        if (attempt > 1) console.log(`  🔄 Retry ${attempt}/${MAX_RETRIES}...`);

        const data = await extractAllInOneVisit(
            item.url,
            browser,
            item.needsMetadata,
            item.existingStoreLink,
            attempt
        );

        if (data.storeLink === 'BLOCKED' || data.appName === 'BLOCKED') return data;

        // If all fields are SKIP (text ad), return immediately - no retries needed
        if (data.storeLink === 'SKIP' && data.appName === 'SKIP') {
            return data;
        }

        // Success criteria: If we needed metadata, did we find it? (at least one of appName or storeLink)
        const metadataSuccess = !item.needsMetadata || (data.storeLink !== 'NOT_FOUND' || data.appName !== 'NOT_FOUND');

        // We return if metadata is successful
        if (metadataSuccess) {
            return data;
        } else {
            console.log(`  ⚠️ Attempt ${attempt} partial success: Metadata=${metadataSuccess}. Retrying...`);
        }

        await randomDelay(1000, 2000); // Reduced retry delay for speed
    }
    // If we're here, we exhausted retries. Return whatever we have.
    return { storeLink: 'NOT_FOUND', appName: 'NOT_FOUND', appSubtitle: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting Google Ads Agent...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Extracting: C=App Link (meta tag), D=App Name, F=App Headline (cS4Vcb-vnv8ic)\n`);

    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000;

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ All rows complete. Nothing to process.');
        process.exit(0);
    }

    const needsMeta = toProcess.filter(x => x.needsMetadata).length;
    console.log(`📊 Found ${toProcess.length} rows to process:`);
    console.log(`   - ${needsMeta} need metadata\n`);

    console.log(PROXIES.length ? `🔁 Proxy rotation enabled (${PROXIES.length} proxies)` : '🔁 Running direct');

    const PAGES_PER_BROWSER = 30; // Balanced: faster but safe
    let currentIndex = 0;

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
                        storeLink: data.storeLink,
                        appName: data.appName,
                        appSubtitle: data.appSubtitle
                    };
                }));

                results.forEach(r => {
                    console.log(`  → Row ${r.rowIndex + 1}: Link=${r.storeLink?.substring(0, 40) || 'NOT_FOUND'}... | Name=${r.appName || 'NOT_FOUND'} | Headline=${r.appSubtitle?.substring(0, 30) || 'NOT_FOUND'}...`);
                });

                // Separate successful results from blocked ones
                const successfulResults = results.filter(r => r.storeLink !== 'BLOCKED' && r.appName !== 'BLOCKED');
                const blockedResults = results.filter(r => r.storeLink === 'BLOCKED' || r.appName === 'BLOCKED');

                // Always write successful results to sheet (even if some were blocked)
                if (successfulResults.length > 0) {
                    await batchWriteToSheet(sheets, successfulResults);
                    console.log(`  ✅ Wrote ${successfulResults.length} successful results to sheet`);
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
                // Adaptive delay: reduce delay more aggressively if we're having success (faster processing)
                const adaptiveMultiplier = Math.max(0.5, 1 - (consecutiveSuccessBatches * 0.08)); // Reduce delay by 8% per successful batch, min 50% (more aggressive)
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