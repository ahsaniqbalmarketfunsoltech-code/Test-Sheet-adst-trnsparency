/**
 * UNIFIED GOOGLE ADS TRANSPARENCY AGENT
 * =====================================
 * Combines app_data_agent.js + agent.js in ONE VISIT per URL
 * 
 * Sheet Structure:
 *   Column A: Advertiser Name
 *   Column B: Ads URL
 *   Column C: App Link
 *   Column D: App Name
 *   Column E: Video ID
 *   Column F: App Subtitle/Tagline
 *   Column G: Image URL
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
const CREDENTIALS_PATH = './credentials.json';
const SHEET_BATCH_SIZE = parseInt(process.env.SHEET_BATCH_SIZE) || 2000; // Rows to load per batch
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 5; // Balanced: faster but safe
const MAX_WAIT_TIME = 60000;
const MAX_RETRIES = 3;
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
    let startRow = 1; // Start from row 2 (skip header)
    let hasMoreData = true;
    let totalProcessed = 0;

    console.log(`📊 Loading data in batches of ${batchSize} rows...`);

    let skippedCount = 0;
    while (hasMoreData) {
        try {
            const endRow = startRow + batchSize - 1;
            const range = `'${SHEET_NAME}'!A${startRow + 1}:G${endRow + 1}`; // +1 because Google Sheets is 1-indexed

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
                const url = (row[1] || '').trim();
                const storeLink = (row[2] || '').trim();
                const appName = row[3]?.trim() || '';
                const videoId = row[4]?.trim() || '';
                const appSubtitle = row[5]?.trim() || '';
                const imageUrl = row[6]?.trim() || '';

                if (!url) continue;

                // SKIP logic: if Column C has any link-like data
                const isNotEmpty = (val) => val && val.length > 5 && val !== 'NOT_FOUND' && val !== 'SKIP' && val !== 'EMPTY';

                if (isNotEmpty(storeLink)) {
                    skippedCount++;
                    continue;
                }

                if (toProcess.length < 3) {
                    console.log(`  🔍 Row ${actualRowIndex + 1} check: URL=${url.substring(0, 30)}... | Link in C="${storeLink}" -> KEEP`);
                }

                toProcess.push({
                    url,
                    rowIndex: actualRowIndex,
                    needsMetadata: true,
                    needsVideoId: false, // We no longer look for video IDs
                    existingStoreLink: ''
                });
            }

            totalProcessed += rows.length;
            process.stdout.write(`  ✓ Scanned ${totalProcessed} rows... found ${toProcess.length} (skipped ${skippedCount})\r`);

            // If we got less than batchSize rows, we've reached the end
            if (rows.length < batchSize) {
                hasMoreData = false;
            } else {
                startRow = endRow + 1;
                // Small delay between batches to avoid rate limits
                await sleep(50);
            }
        } catch (error) {
            console.error(`\n  ⚠️ Error loading batch starting at row ${startRow}: ${error.message}`);
            // If error, try to continue with next batch
            startRow += batchSize;
            await sleep(500); // Wait a bit longer on error
        }
    }
    console.log(`\n📊 Total: ${totalProcessed} scanned, ${toProcess.length} to process, ${skippedCount} items already have links.`);
    return toProcess;
}

async function batchWriteToSheet(sheets, updates) {
    if (updates.length === 0) return;

    const data = [];
    updates.forEach(({ rowIndex, advertiserName, storeLink, appName, videoId, appSubtitle, imageUrl }) => {
        const rowNum = rowIndex + 1;

        // Only write advertiser name if it's found and not a generic skip/error
        if (advertiserName && !['SKIP', 'NOT_FOUND', 'BLOCKED', 'ERROR'].includes(advertiserName)) {
            data.push({ range: `'${SHEET_NAME}'!A${rowNum}`, values: [[advertiserName]] });
        }

        if (storeLink && storeLink !== 'SKIP' && storeLink !== 'ERROR') {
            data.push({ range: `'${SHEET_NAME}'!C${rowNum}`, values: [[storeLink]] });
        }

        if (appName && appName !== 'SKIP' && appName !== 'ERROR') {
            data.push({ range: `'${SHEET_NAME}'!D${rowNum}`, values: [[appName]] });
        }

        if (videoId && videoId !== 'SKIP' && videoId !== 'ERROR') {
            data.push({ range: `'${SHEET_NAME}'!E${rowNum}`, values: [[videoId]] });
        }

        // Subtitle and Image
        if (appSubtitle && appSubtitle !== 'SKIP') {
            data.push({ range: `'${SHEET_NAME}'!F${rowNum}`, values: [[appSubtitle]] });
        }
        if (imageUrl && imageUrl !== 'SKIP') {
            data.push({ range: `'${SHEET_NAME}'!G${rowNum}`, values: [[imageUrl]] });
        }

        // Timestamp
        const timestamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
        data.push({ range: `'${SHEET_NAME}'!M${rowNum}`, values: [[timestamp]] });
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
// Both metadata + video ID extracted on same page
// ============================================
async function extractAllInOneVisit(url, browser, needsMetadata, needsVideoId, existingStoreLink, attempt = 1) {
    const page = await browser.newPage();
    let result = {
        advertiserName: 'SKIP',
        appName: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        storeLink: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        videoId: 'SKIP',
        appSubtitle: needsMetadata ? 'NOT_FOUND' : 'SKIP',
        imageUrl: needsMetadata ? 'NOT_FOUND' : 'SKIP'
    };
    let capturedVideoId = null;

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

    // SPEED OPTIMIZATION (BLOCK UNNECESSARY RESOURCES)
    await page.setRequestInterception(true);
    page.on('request', (request) => {
        const requestUrl = request.url();
        const resourceType = request.resourceType();

        // Block heavy/tracking resources
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
            return { advertiserName: 'BLOCKED', appName: 'BLOCKED', storeLink: 'BLOCKED', videoId: 'BLOCKED' };
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

        // All ads (video, text, image) will now be processed.

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
                        const data = { appName: null, storeLink: null, isVideo: false, appSubtitle: null, imageUrl: null };
                        const root = document.querySelector('#portrait-landscape-phone') || document.body;

                        // Check if this frame content is visible (has dimensions)
                        const bodyRect = document.body.getBoundingClientRect();
                        if (bodyRect.width < 50 || bodyRect.height < 50) {
                            return { ...data, isHidden: true };
                        }

                        // =====================================================
                        // EXTRACT APP SUBTITLE/TAGLINE (for text ads: div.c54Vcb-vmv8lc)
                        // =====================================================
                        const subtitleSelectors = [
                            '.c54Vcb-vmv8lc',           // Text ad headline (priority)
                            'div.c54Vcb-vmv8lc',        // Explicit text ad headline
                            '.ns-yp8c1-e-18.headline',  // Video ad headline
                            '[class*="yp8c1"][class*="headline"]',
                            '.headline',
                            '.ad-title',
                            '[class*="ad-title"]',
                            '[class*="vmv8lc"]',
                            '[class*="tagline"]',
                            '[class*="subtitle"]',
                            '.header-text',
                            'h1, h2, h3' // Final fallbacks
                        ];
                        // First try on root, then on document
                        for (const searchRoot of [root, document]) {
                            if (data.appSubtitle && data.appSubtitle !== 'NOT_FOUND') break;
                            for (const sel of subtitleSelectors) {
                                try {
                                    const elements = searchRoot.querySelectorAll(sel);
                                    for (const el of elements) {
                                        const text = (el.innerText || el.textContent || '').trim();
                                        // Relaxed validation
                                        if (text && text.length >= 3 && text.length <= 250) {
                                            if (!/^[\d\s\W]+$/.test(text) && !text.includes('{') && !text.includes('<')) {
                                                // Check if it's not the advertiser name
                                                if (text.toLowerCase() !== blacklist) {
                                                    data.appSubtitle = text;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if (data.appSubtitle) break;
                                } catch (e) { }
                            }
                        }

                        // =====================================================
                        // EXTRACT IMAGE URL (especially for Image Ads)
                        // =====================================================
                        const imageSelectors = [
                            '.html-container img',
                            '.creative-container img',
                            'img[src*="googlesyndication.com"]',
                            'img[src*="archive/simad"]',
                            'img[src*="simad"]'
                        ];
                        for (const sel of imageSelectors) {
                            try {
                                const img = root.querySelector(sel);
                                if (img && img.src && img.src.startsWith('http')) {
                                    data.imageUrl = img.src;
                                    break;
                                }
                            } catch (e) { }
                        }

                        // =====================================================
                        // ULTRA-PRECISE STORE LINK EXTRACTOR
                        // Only accepts REAL Play Store / App Store links
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
                            } catch (e) { }

                            return null;
                        };

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

                            // Blacklist for app names
                            const blacklistNames = [
                                'ad details', 'google ads', 'transparency center', 'about this ad',
                                'privacy policy', 'terms of service', 'install now', 'download',
                                'play store', 'app store', 'advertisement', 'sponsored',
                                'open', 'get', 'visit', 'learn more'
                            ];
                            if (blacklistNames.some(name => clean.toLowerCase() === name || clean.toLowerCase().includes(name))) return null;

                            if (clean.length < 2 || clean.length > 80) return null;
                            if (/^[\d\s\W]+$/.test(clean)) return null;
                            return clean;
                        };

                        // =====================================================
                        // EXTRACTION - Find FIRST element with BOTH name + store link
                        // Uses PRECISE selectors from app_data_agent.js
                        // =====================================================
                        const appNameSelectors = [
                            'a[data-asoch-targets*="ochAppName"]',
                            'a[data-asoch-targets*="appname" i]',
                            'a[data-asoch-targets*="rrappname" i]',
                            '.ad-header-text',         // Text ad header
                            '[class*="header-text"]',
                            '[class*="short-app-name"]',
                            '.short-app-name a',
                            '.visible-app-name',
                            '[class*="app-name"]',
                            '[class*="appName"]',
                            '.app-name',
                            '.ad-listing-title',
                            '[aria-label*="appName" i]',
                            'div[class*="header"] > span'
                        ];

                        for (const selector of appNameSelectors) {
                            const elements = root.querySelectorAll(selector);
                            for (const el of elements) {
                                const rawName = el.innerText || el.textContent || '';
                                const appName = cleanAppName(rawName);
                                if (!appName || appName.toLowerCase() === blacklist) continue;

                                const storeLink = extractStoreLink(el.href);
                                if (appName && storeLink) {
                                    return { appName, storeLink, isVideo: true, isHidden: false };
                                } else if (appName && !data.appName) {
                                    data.appName = appName;
                                }
                            }
                        }

                        // Backup: Install button for link
                        if (data.appName && !data.storeLink) {
                            const installSels = [
                                'a[data-asoch-targets*="ochButton"]',
                                'a[data-asoch-targets*="Install" i]',
                                'a[aria-label*="Install" i]'
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
                        // TEXT AD STORE LINK EXTRACTION (from meta tag)
                        // For text ads, the package name is in <meta data-asoch-meta>
                        // NOTE: meta tags are in <head>, so we search document not root
                        // =====================================================
                        if (!data.storeLink) {
                            // Search for meta tag with data-asoch-meta attribute - search DOCUMENT not root (meta is in head)
                            const metaElements = document.querySelectorAll('meta[data-asoch-meta], *[data-asoch-meta]');
                            for (const meta of metaElements) {
                                const metaValue = meta.getAttribute('data-asoch-meta') || '';

                                // Method 1: Look for full play.google.com URL with id= parameter
                                if (metaValue.includes('play.google.com')) {
                                    const match = metaValue.match(/id=([a-zA-Z0-9._]+)/);
                                    if (match && match[1]) {
                                        data.storeLink = `https://play.google.com/store/apps/details?id=${match[1]}`;
                                        break;
                                    }
                                }

                                // Method 2: Look for adurl with encoded Play Store URL
                                if (metaValue.includes('adurl=')) {
                                    const adurlMatch = metaValue.match(/adurl=([^"'\s\]]+)/);
                                    if (adurlMatch && adurlMatch[1]) {
                                        try {
                                            const decodedUrl = decodeURIComponent(adurlMatch[1]);
                                            const pkgMatch = decodedUrl.match(/id=([a-zA-Z0-9._]+)/);
                                            if (pkgMatch && pkgMatch[1]) {
                                                data.storeLink = `https://play.google.com/store/apps/details?id=${pkgMatch[1]}`;
                                                break;
                                            }
                                        } catch (e) { }
                                    }
                                }

                                // Method 3: Look for package name pattern directly
                                // Package names like: com.walk.walkwin, com.app.game, etc.
                                const pkgMatches = metaValue.match(/["']([a-z][a-z0-9_]*(\.[a-z0-9_]+){2,})["']/gi);
                                if (pkgMatches) {
                                    for (const pkg of pkgMatches) {
                                        const cleanPkg = pkg.replace(/["']/g, '');
                                        // Filter out common non-app packages
                                        if (!cleanPkg.includes('google') &&
                                            !cleanPkg.includes('example') &&
                                            !cleanPkg.includes('android.') &&
                                            !cleanPkg.startsWith('com.google') &&
                                            cleanPkg.split('.').length >= 3) {
                                            data.storeLink = `https://play.google.com/store/apps/details?id=${cleanPkg}`;
                                            break;
                                        }
                                    }
                                    if (data.storeLink) break;
                                }
                            }
                        }

                        // Also search all links for store URLs (comprehensive backup)
                        if (!data.storeLink) {
                            const allLinks = root.querySelectorAll('a[href]');
                            for (const link of allLinks) {
                                const href = link.getAttribute('href') || link.href || '';
                                const storeLink = extractStoreLink(href);
                                if (storeLink) {
                                    data.storeLink = storeLink;
                                    break;
                                }
                            }
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

                    // Extract app subtitle if found
                    if (frameData.appSubtitle && result.appSubtitle === 'NOT_FOUND') {
                        result.appSubtitle = frameData.appSubtitle;
                        console.log(`  ✓ Found subtitle: ${result.appSubtitle.substring(0, 50)}...`);
                    }

                    // Extract Image URL if found
                    if (frameData.imageUrl && result.imageUrl === 'NOT_FOUND') {
                        result.imageUrl = frameData.imageUrl;
                        console.log(`  🖼️ Found Image URL: ${result.imageUrl.substring(0, 50)}...`);
                    }

                    // Skip hidden frames
                    if (frameData.isHidden) continue;

                    // If we found BOTH app name AND store link, use this immediately (high confidence)
                    if (frameData.appName && frameData.storeLink && result.appName === 'NOT_FOUND') {
                        result.appName = cleanName(frameData.appName);
                        result.storeLink = frameData.storeLink;
                        console.log(`  ✓ Found: ${result.appName} -> ${result.storeLink.substring(0, 60)}...`);
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

            // =====================================================
            // FALLBACK FOR SUBTITLE/HEADLINE (Main Page)
            // For text ads, headline is in div.c54Vcb-vmv8lc on main page
            // =====================================================
            if (result.appSubtitle === 'NOT_FOUND') {
                const mainSubtitle = await page.evaluate(() => {
                    const selectors = [
                        '.c54Vcb-vmv8lc',           // Text ad headline
                        'div.c54Vcb-vmv8lc',        // Explicit div selector
                        '[class*="vmv8lc"]',        // Partial class match
                        '[class*="tagline"]',
                        '[class*="subtitle"]',
                        '.headline'
                    ];
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el) {
                            const text = (el.innerText || el.textContent || '').trim();
                            // Validate text: not too short, not too long, not CSS garbage
                            if (text && text.length >= 3 && text.length <= 150) {
                                if (!/^[\d\s\W]+$/.test(text) && !text.includes('{') && !text.includes(':')) {
                                    return text;
                                }
                            }
                        }
                    }
                    return null;
                });
                if (mainSubtitle) {
                    result.appSubtitle = mainSubtitle;
                    console.log(`  ✓ Found headline (main page): ${mainSubtitle.substring(0, 50)}...`);
                }
            }

            // =====================================================
            // FALLBACK FOR STORE LINK (Main Page - for text ads)
            // Search main page for meta data-asoch-meta with package info
            // =====================================================
            if (result.storeLink === 'NOT_FOUND') {
                const mainStoreLink = await page.evaluate(() => {
                    // Look for meta tag with data-asoch-meta
                    const metaElements = document.querySelectorAll('meta[data-asoch-meta], div[data-asoch-meta], *[data-asoch-meta]');
                    for (const meta of metaElements) {
                        const metaValue = meta.getAttribute('data-asoch-meta') || '';

                        // Look for Play Store URL in the meta value
                        if (metaValue.includes('play.google.com')) {
                            const match = metaValue.match(/id=([a-zA-Z0-9._]+)/);
                            if (match && match[1]) {
                                return `https://play.google.com/store/apps/details?id=${match[1]}`;
                            }
                        }

                        // Look for encoded adurl
                        if (metaValue.includes('adurl=')) {
                            const adurlMatch = metaValue.match(/adurl=([^"'\s\]]+)/);
                            if (adurlMatch && adurlMatch[1]) {
                                try {
                                    const decodedUrl = decodeURIComponent(adurlMatch[1]);
                                    const pkgMatch = decodedUrl.match(/id=([a-zA-Z0-9._]+)/);
                                    if (pkgMatch && pkgMatch[1]) {
                                        return `https://play.google.com/store/apps/details?id=${pkgMatch[1]}`;
                                    }
                                } catch (e) { }
                            }
                        }

                        // Direct package name pattern
                        const pkgMatches = metaValue.match(/["']([a-z][a-z0-9_]*(\.[a-z0-9_]+){2,})["']/gi);
                        if (pkgMatches) {
                            for (const pkg of pkgMatches) {
                                const cleanPkg = pkg.replace(/["']/g, '');
                                if (!cleanPkg.includes('google') &&
                                    !cleanPkg.includes('example') &&
                                    !cleanPkg.includes('android.') &&
                                    !cleanPkg.startsWith('com.google') &&
                                    cleanPkg.split('.').length >= 3) {
                                    return `https://play.google.com/store/apps/details?id=${cleanPkg}`;
                                }
                            }
                        }
                    }
                    return null;
                });
                if (mainStoreLink) {
                    result.storeLink = mainStoreLink;
                    console.log(`  ✓ Found store link (main page): ${mainStoreLink.substring(0, 60)}...`);
                }
            }

            // =====================================================
            // RAW HTML CONTENT FALLBACK (for cross-origin iframes)
            // Search the full page HTML + iframe contents for package names
            // =====================================================
            if (result.storeLink === 'NOT_FOUND') {
                console.log(`  🔍 Searching HTML content (main + iframes) for store link...`);
                try {
                    // Collect HTML from main page AND all accessible frames
                    let allContent = await page.content();

                    // Get content from all frames
                    const frames = page.frames();
                    console.log(`  📄 Found ${frames.length} frames to search`);

                    let accessibleFrames = 0;
                    for (const frame of frames) {
                        try {
                            const frameUrl = frame.url();
                            const frameContent = await frame.content();
                            allContent += frameContent;
                            accessibleFrames++;
                            // Debug: check if this frame has the meta tag
                            if (frameContent.includes('data-asoch-meta')) {
                                console.log(`  ✅ Frame has data-asoch-meta: ${frameUrl.substring(0, 50)}...`);
                            }
                        } catch (e) {
                            // Log which frames are inaccessible
                            console.log(`  ⚠️ Cannot access frame: ${frame.url().substring(0, 50)}...`);
                        }
                    }
                    console.log(`  📄 Accessible frames: ${accessibleFrames}/${frames.length}`);
                    console.log(`  📄 Total HTML content length: ${allContent.length} chars`);

                    // Check if data-asoch-meta exists
                    const hasAsochMeta = allContent.includes('data-asoch-meta');
                    console.log(`  📱 Has data-asoch-meta: ${hasAsochMeta}`);

                    // Method 1: Look for play.google.com/store/apps/details?id= pattern
                    const playStoreMatches = allContent.match(/play\.google\.com\/store\/apps\/details\?id=([a-zA-Z0-9._]+)/g);
                    console.log(`  📱 Play Store direct matches: ${playStoreMatches ? playStoreMatches.length : 0}`);
                    if (playStoreMatches && playStoreMatches.length > 0) {
                        const idMatch = playStoreMatches[0].match(/id=([a-zA-Z0-9._]+)/);
                        if (idMatch && idMatch[1]) {
                            result.storeLink = `https://play.google.com/store/apps/details?id=${idMatch[1]}`;
                            console.log(`  ✓ Found store link (HTML content): ${result.storeLink.substring(0, 60)}...`);
                        }
                    }

                    // Method 2: Look for encoded adurl with package
                    if (result.storeLink === 'NOT_FOUND') {
                        const adurlMatches = allContent.match(/adurl=https?%3A%2F%2Fplay\.google\.com%2Fstore%2Fapps%2Fdetails%3Fid%3D([a-zA-Z0-9._]+)/g);
                        console.log(`  📱 Encoded adurl matches: ${adurlMatches ? adurlMatches.length : 0}`);
                        if (adurlMatches && adurlMatches.length > 0) {
                            const idMatch = adurlMatches[0].match(/id%3D([a-zA-Z0-9._]+)/);
                            if (idMatch && idMatch[1]) {
                                result.storeLink = `https://play.google.com/store/apps/details?id=${idMatch[1]}`;
                                console.log(`  ✓ Found store link (encoded adurl): ${result.storeLink.substring(0, 60)}...`);
                            }
                        }
                    }

                    // Method 3: Look for data-asoch-meta containing package info
                    if (result.storeLink === 'NOT_FOUND' && hasAsochMeta) {
                        // Try to find package pattern in the meta content
                        const metaMatch = allContent.match(/data-asoch-meta="[^"]*?id%3D([a-zA-Z0-9._]+)/);
                        if (metaMatch && metaMatch[1] && !metaMatch[1].includes('google')) {
                            result.storeLink = `https://play.google.com/store/apps/details?id=${metaMatch[1]}`;
                            console.log(`  ✓ Found store link (meta encoded): ${result.storeLink.substring(0, 60)}...`);
                        }
                    }

                    // Method 4: Look for any package name pattern (com.xxx.xxx)
                    if (result.storeLink === 'NOT_FOUND') {
                        // Search for patterns like "com.walk.walkwin" in the HTML
                        const packagePattern = /["\s]([a-z][a-z0-9_]*\.[a-z0-9_]+\.[a-z0-9._]+)["\s,\]]/gi;
                        const allPackages = [...allContent.matchAll(packagePattern)];
                        const validPackages = allPackages
                            .map(m => m[1])
                            .filter(pkg =>
                                pkg.startsWith('com.') &&
                                !pkg.includes('google') &&
                                !pkg.includes('android') &&
                                pkg.split('.').length >= 3
                            );
                        console.log(`  📱 Package name patterns found: ${validPackages.length}`);
                        if (validPackages.length > 0) {
                            result.storeLink = `https://play.google.com/store/apps/details?id=${validPackages[0]}`;
                            console.log(`  ✓ Found store link (package pattern): ${result.storeLink.substring(0, 60)}...`);
                        }
                    }
                } catch (e) {
                    console.log(`  ⚠️ HTML content search failed: ${e.message}`);
                }
            }
        }

        // Video ID is no longer requested
        result.videoId = 'NOT_FOUND';

        await page.close();
        return result;
    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        await page.close();
        return { advertiserName: 'ERROR', appName: 'ERROR', storeLink: 'ERROR', videoId: 'ERROR' };
    }
}

async function extractWithRetry(item, browser) {
    let bestResult = null;

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

        // Update bestResult with any non-NOT_FOUND data we find
        if (!bestResult) {
            bestResult = { ...data };
        } else {
            // Merge results: keep what we already found if new attempt returns NOT_FOUND
            if (data.advertiserName !== 'NOT_FOUND' && data.advertiserName !== 'SKIP') bestResult.advertiserName = data.advertiserName;
            if (data.appName !== 'NOT_FOUND' && data.appName !== 'SKIP') bestResult.appName = data.appName;
            if (data.storeLink !== 'NOT_FOUND' && data.storeLink !== 'SKIP') bestResult.storeLink = data.storeLink;
            if (data.videoId !== 'NOT_FOUND' && data.videoId !== 'SKIP') bestResult.videoId = data.videoId;
            if (data.appSubtitle !== 'NOT_FOUND' && data.appSubtitle !== 'SKIP') bestResult.appSubtitle = data.appSubtitle;
            if (data.imageUrl !== 'NOT_FOUND' && data.imageUrl !== 'SKIP') bestResult.imageUrl = data.imageUrl;
        }

        // If all fields are SKIP, return immediately
        if (data.storeLink === 'SKIP' && data.appName === 'SKIP' && data.videoId === 'SKIP') {
            return bestResult;
        }

        // Determine if we have a valid store link
        const currentStoreLink = (bestResult.storeLink && bestResult.storeLink !== 'SKIP' && bestResult.storeLink !== 'NOT_FOUND')
            ? bestResult.storeLink
            : item.existingStoreLink;

        const isPlayStore = currentStoreLink && currentStoreLink.includes('play.google.com');
        const isAppleStore = currentStoreLink && currentStoreLink.includes('apps.apple.com');

        // Success criteria (METADATA ONLY):
        const hasName = bestResult.appName && bestResult.appName !== 'NOT_FOUND' && bestResult.appName !== 'SKIP';
        const hasLink = bestResult.storeLink && bestResult.storeLink !== 'NOT_FOUND' && bestResult.storeLink !== 'SKIP';
        const hasSubtitle = bestResult.appSubtitle && bestResult.appSubtitle !== 'NOT_FOUND' && bestResult.appSubtitle !== 'SKIP';
        const hasImage = bestResult.imageUrl && bestResult.imageUrl !== 'NOT_FOUND' && bestResult.imageUrl !== 'SKIP';

        const success = !item.needsMetadata || (hasName && hasLink);

        if (success) {
            return bestResult;
        } else {
            console.log(`  ⚠️ Attempt ${attempt} result: Success=${success}`);
        }

        await randomDelay(1500, 3000);
    }

    return bestResult || { advertiserName: 'NOT_FOUND', storeLink: 'NOT_FOUND', appName: 'NOT_FOUND', videoId: 'NOT_FOUND', appSubtitle: 'NOT_FOUND', imageUrl: 'NOT_FOUND' };
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log(`🤖 Starting UNIFIED Google Ads Agent...\n`);
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`⚡ Columns: A=Advertiser, B=URL, C=App Link, D=App Name, E=Video ID, F=Subtitle, G=Image URL\n`);

    const sessionStartTime = Date.now();
    const MAX_RUNTIME = 330 * 60 * 1000;

    const sheets = await getGoogleSheetsClient();
    const toProcess = await getUrlData(sheets);

    if (toProcess.length === 0) {
        console.log('✨ All rows complete. Nothing to process.');
        process.exit(0);
    }

    const needsMeta = toProcess.filter(x => x.needsMetadata).length;
    const needsVideo = toProcess.filter(x => x.needsVideoId).length;
    console.log(`📊 Found ${toProcess.length} rows to process:`);
    console.log(`   - ${needsMeta} need metadata`);
    console.log(`   - ${needsVideo} need video ID\n`);

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
            '--disable-extensions',
            '--disable-background-networking',
            '--no-first-run',
            '--window-size=1280,720'
        ];

        const proxy = pickProxy();
        if (proxy) launchArgs.push(`--proxy-server=${proxy}`);

        console.log(`  🌐 Browser (proxy: ${proxy || 'DIRECT'})`);

        let browser;
        try {
            browser = await puppeteer.launch({
                headless: 'new',
                args: launchArgs,
                timeout: 60000,
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
                    // Staggered starts
                    if (index > 0) {
                        const staggerDelay = PAGE_LOAD_DELAY_MIN + Math.random() * (PAGE_LOAD_DELAY_MAX - PAGE_LOAD_DELAY_MIN);
                        await sleep(staggerDelay * index);
                    }

                    console.log(`  🚀 Row ${item.rowIndex + 1}: Starting ${item.url.substring(0, 40)}...`);
                    const data = await extractWithRetry(item, browser);

                    return {
                        url: item.url,
                        rowIndex: item.rowIndex,
                        advertiserName: data.advertiserName,
                        storeLink: data.storeLink,
                        appName: data.appName,
                        videoId: data.videoId,
                        appSubtitle: data.appSubtitle,
                        imageUrl: data.imageUrl
                    };
                }));

                results.forEach(r => {
                    const linkStatus = r.storeLink && r.storeLink !== 'NOT_FOUND' && r.storeLink !== 'BLOCKED' ? '✅' : '❌';
                    console.log(`  ${linkStatus} Row ${r.rowIndex + 1}: Link=${r.storeLink?.substring(0, 45) || 'NONE'} | Name=${r.appName}`);
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