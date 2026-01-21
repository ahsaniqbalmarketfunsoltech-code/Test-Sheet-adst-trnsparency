/**
 * HTML PARSER TEST AGENT
 * ======================
 * SAFETY FIRST - Maximum stealth and anti-detection
 * 
 * This test agent:
 * - Connects to Google Sheets
 * - Processes URLs ONE BY ONE (safe, slow, thorough)
 * - Captures FULL HTML source (including scripts, excluding CSS)
 * - Saves HTML to files for inspection
 * - Waits for user to identify data locations
 */

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
puppeteer.use(StealthPlugin());
const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// ============================================
// CONFIGURATION - SAFETY FIRST
// ============================================
const SPREADSHEET_ID = '1l4JpCcA1GSkta1CE77WxD_YCgePHI87K7NtMu1Sd4Q0';
const SHEET_NAME = process.env.SHEET_NAME || 'Test';
const CREDENTIALS_PATH = './credentials.json';
const HTML_OUTPUT_DIR = process.env.HTML_OUTPUT_DIR || ''; // Optional: Set to save HTML files (empty = disabled)
const MAX_URLS_TO_TEST = parseInt(process.env.MAX_URLS_TO_TEST) || 5; // Test only first N URLs

// SAFETY SETTINGS - Maximum delays and stealth
const PAGE_LOAD_DELAY = parseInt(process.env.PAGE_LOAD_DELAY) || 5000; // 5 seconds between pages
const WAIT_AFTER_LOAD = parseInt(process.env.WAIT_AFTER_LOAD) || 8000; // 8 seconds wait after page load
const MAX_WAIT_TIME = 90000; // 90 seconds timeout
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY) || 10000; // 10 seconds between retries
const MAX_RETRIES = 2; // Only 2 retries for safety

// Proxy settings (if needed)
const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

// User agents for rotation
const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
];

const VIEWPORTS = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768 },
    { width: 1536, height: 864 },
    { width: 1440, height: 900 }
];

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const randomDelay = (min, max) => new Promise(r => setTimeout(r, min + Math.random() * (max - min)));

// ============================================
// GOOGLE SHEETS
// ============================================
async function getGoogleSheetsClient() {
    if (!fs.existsSync(CREDENTIALS_PATH)) {
        throw new Error(`Credentials file not found: ${CREDENTIALS_PATH}`);
    }
    const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const authClient = await auth.getClient();
    return google.sheets({ version: 'v4', auth: authClient });
}

async function getUrlData(sheets) {
    const toProcess = [];
    const range = `${SHEET_NAME}!A2:G1000`; // Get first 1000 rows (A-G columns)

    try {
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: range,
        });

        const rows = response.data.values || [];

        for (let i = 0; i < rows.length && toProcess.length < MAX_URLS_TO_TEST; i++) {
            const row = rows[i];
            const actualRowIndex = i + 1; // Row 2 = index 1 (skip header)
            const url = row[1]?.trim() || ''; // Column B (index 1)
            const storeLink = row[2]?.trim() || ''; // Column C (index 2)
            const appName = row[3]?.trim() || ''; // Column D (index 3)
            const appSubtitle = row[5]?.trim() || ''; // Column F (index 5)

            if (!url) continue;

            // SKIP: Rows with Play Store link in Column C (same logic as unified_agent)
            const hasPlayStoreLink = storeLink && storeLink.includes('play.google.com');
            if (hasPlayStoreLink) {
                continue; // Skip - already has Play Store link
            }

            // Process rows that need extraction (app name, app link, or app headline)
            const needsMetadata = !storeLink || storeLink === 'NOT_FOUND' || !appName || !appSubtitle;
            
            toProcess.push({
                url,
                rowIndex: actualRowIndex,
                rowNumber: actualRowIndex + 1, // Actual sheet row number
                needsMetadata,
                existingStoreLink: storeLink,
                existingAppName: appName,
                existingAppSubtitle: appSubtitle
            });
        }

        console.log(`📊 Found ${toProcess.length} URLs to test\n`);
        return toProcess;
    } catch (error) {
        console.error(`❌ Error loading sheet data: ${error.message}`);
        throw error;
    }
}

// ============================================
// HTML PARSING FUNCTIONS
// ============================================
function removeCSSFromHTML(html) {
    // Remove <style> tags and their content
    html = html.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '');
    
    // Remove style attributes from elements
    html = html.replace(/\s*style\s*=\s*["'][^"']*["']/gi, '');
    
    // Remove CSS-related link tags
    html = html.replace(/<link[^>]*rel\s*=\s*["']stylesheet["'][^>]*>/gi, '');
    
    return html;
}

function extractScripts(html) {
    const $ = cheerio.load(html);
    const scripts = [];
    
    $('script').each((i, elem) => {
        const scriptContent = $(elem).html() || '';
        const scriptSrc = $(elem).attr('src') || '';
        
        if (scriptContent.trim()) {
            scripts.push({
                type: 'inline',
                content: scriptContent,
                index: i
            });
        } else if (scriptSrc) {
            scripts.push({
                type: 'external',
                src: scriptSrc,
                index: i
            });
        }
    });
    
    return scripts;
}

// Write to sheet - ONLY Columns C, D, F
// Column C: Full Play Store URL (https://play.google.com/store/apps/details?id=com.example.app)
// Column D: App Name
// Column F: App Subtitle/Headline
async function writeToSheet(sheets, rowIndex, packageName, appName, appSubtitle) {
    const rowNum = rowIndex + 1; // Convert 0-based index to 1-based row number
    const data = [];
    
    // Play Store URL template
    const PLAY_STORE_URL_TEMPLATE = 'https://play.google.com/store/apps/details?id=';
    
    // Construct full Play Store URL (Column C) - Combine template + package name
    let storeLinkValue = 'NOT_FOUND';
    if (packageName && packageName !== 'SKIP' && packageName !== 'NOT_FOUND') {
        storeLinkValue = `${PLAY_STORE_URL_TEMPLATE}${packageName}`;
    }
    data.push({ range: `${SHEET_NAME}!C${rowNum}`, values: [[storeLinkValue]] });
    
    // Write app name (Column D) - ALWAYS write
    const appNameValue = appName && appName !== 'SKIP' ? appName : 'NOT_FOUND';
    data.push({ range: `${SHEET_NAME}!D${rowNum}`, values: [[appNameValue]] });
    
    // Write app headline/subtitle (Column F) - ALWAYS write
    const appSubtitleValue = appSubtitle || 'NOT_FOUND';
    data.push({ range: `${SHEET_NAME}!F${rowNum}`, values: [[appSubtitleValue]] });
    
    if (data.length === 0) return;
    
    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: { valueInputOption: 'RAW', data: data }
        });
        console.log(`  ✅ Wrote to sheet: Row ${rowNum} - C=${storeLinkValue.substring(0, 60)}... | D=${appNameValue} | F=${appSubtitleValue?.substring(0, 30)}...`);
    } catch (error) {
        console.error(`  ❌ Write error:`, error.message);
    }
}

// Optional: Save HTML to file for inspection (can be disabled)
function saveHTMLToFile(html, url, rowNumber) {
    // Only save if HTML_OUTPUT_DIR is set and directory exists
    if (!HTML_OUTPUT_DIR || HTML_OUTPUT_DIR === '') return null;
    
    // Create output directory if it doesn't exist
    if (!fs.existsSync(HTML_OUTPUT_DIR)) {
        fs.mkdirSync(HTML_OUTPUT_DIR, { recursive: true });
    }
    
    // Clean URL for filename
    const urlSlug = url.replace(/https?:\/\//, '').replace(/[^a-zA-Z0-9]/g, '_').substring(0, 50);
    const filename = `row_${rowNumber}_${urlSlug}.html`;
    const filepath = path.join(HTML_OUTPUT_DIR, filename);
    
    fs.writeFileSync(filepath, html, 'utf8');
    return filepath;
}

// ============================================
// ENHANCED ANTI-DETECTION SETUP
// ============================================
async function setupAntiDetection(page, userAgent, viewport) {
    // Set user agent
    await page.setUserAgent(userAgent);
    
    // Set viewport
    await page.setViewport(viewport);
    
    // Random screen properties
    const screenWidth = viewport.width + Math.floor(Math.random() * 100) - 50;
    const screenHeight = viewport.height + Math.floor(Math.random() * 100) - 50;
    
    // Enhanced fingerprint masking
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
        
        // Hardware concurrency
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 4 + Math.floor(Math.random() * 4),
            configurable: true
        });
        
        // Device memory
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
        
        // Canvas fingerprint protection
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
    
    // Block unnecessary resources (CSS, images, fonts) for speed
    await page.setRequestInterception(true);
    page.on('request', (request) => {
        const requestUrl = request.url();
        const resourceType = request.resourceType();
        
        // Block CSS, images, fonts, but keep scripts and HTML
        const blockedTypes = ['image', 'font', 'stylesheet'];
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
    
    // Enhanced headers
    const acceptLanguages = [
        'en-US,en;q=0.9',
        'en-US,en;q=0.9,zh-CN;q=0.8',
        'en-GB,en;q=0.9'
    ];
    
    await page.setExtraHTTPHeaders({
        'accept-language': acceptLanguages[Math.floor(Math.random() * acceptLanguages.length)],
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
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
}

// ============================================
// EXTRACT FULL HTML
// ============================================
async function extractFullHTML(url, browser, attempt = 1) {
    const page = await browser.newPage();
    
    try {
        const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
        const viewport = VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)];
        
        await setupAntiDetection(page, userAgent, viewport);
        
        console.log(`\n🌐 [Attempt ${attempt}] Loading: ${url.substring(0, 60)}...`);
        console.log(`   User-Agent: ${userAgent.substring(0, 50)}...`);
        console.log(`   Viewport: ${viewport.width}x${viewport.height}`);
        
        // Human-like mouse movement before load
        try {
            const client = await page.target().createCDPSession();
            await client.send('Input.dispatchMouseEvent', {
                type: 'mouseMoved',
                x: Math.random() * viewport.width,
                y: Math.random() * viewport.height
            });
        } catch (e) { /* Ignore */ }
        
        // Navigate with networkidle2 for full page load
        const response = await page.goto(url, {
            waitUntil: 'networkidle2',
            timeout: MAX_WAIT_TIME
        });
        
        // Check for blocks
        const content = await page.content();
        if ((response && response.status && response.status() === 429) ||
            content.includes('Our systems have detected unusual traffic') ||
            content.includes('Too Many Requests') ||
            content.toLowerCase().includes('captcha') ||
            content.toLowerCase().includes('g-recaptcha') ||
            content.toLowerCase().includes('verify you are human')) {
            console.error('  ⚠️ BLOCKED - Rate limit or CAPTCHA detected');
            await page.close();
            return { success: false, blocked: true, html: null };
        }
        
        // Wait for dynamic content (SAFETY: Long wait)
        console.log(`   ⏳ Waiting ${WAIT_AFTER_LOAD / 1000}s for dynamic content...`);
        await sleep(WAIT_AFTER_LOAD);
        
        // Wait for iframes to load
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
                                setTimeout(resolve, 2000);
                            }
                        };
                        iframes.forEach(iframe => {
                            try {
                                if (iframe.contentDocument && iframe.contentDocument.readyState === 'complete') {
                                    checkLoaded();
                                } else {
                                    iframe.onload = checkLoaded;
                                    setTimeout(checkLoaded, 3000);
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
        
        // Human-like scrolling
        await page.evaluate(async () => {
            for (let i = 0; i < 3; i++) {
                window.scrollBy(0, 300 + Math.random() * 200);
                await new Promise(r => setTimeout(r, 300 + Math.random() * 200));
            }
            window.scrollBy(0, -200);
            await new Promise(r => setTimeout(r, 300));
        });
        
        // Get FULL HTML source
        const fullHTML = await page.content();
        
        // Remove CSS (as requested)
        const htmlWithoutCSS = removeCSSFromHTML(fullHTML);
        
        // Extract scripts
        const scripts = extractScripts(htmlWithoutCSS);
        
        // Parse HTML and extract data (using cheerio) - EXACT SAME LOGIC AS unified_agent
        const $ = cheerio.load(htmlWithoutCSS);
        
        // Clean name function - EXACT COPY from unified_agent
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
        
        // Clean name function with more CSS filtering - EXACT COPY from unified_agent
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
        
        const blacklistName = 'ad details';
        let packageName = null; // Extract ONLY package name (e.g., com.example.app)
        let appName = null;
        let appSubtitle = null;
        
        // Extract package name from meta tag - EXACT SAME LOGIC as unified_agent, but return ONLY package name
        try {
            const firstMetaTag = $('meta[data-asoch-meta]').first();
            if (firstMetaTag.length > 0) {
                const metaData = firstMetaTag.attr('data-asoch-meta');
                if (metaData) {
                    // Pattern 1: Direct regex extraction (fastest)
                    const fastPackageMatch = metaData.match(/id%3D([a-zA-Z0-9._]+)|[?&]id=([a-zA-Z0-9._]+)/);
                    if (fastPackageMatch) {
                        const pkgName = fastPackageMatch[1] || fastPackageMatch[2];
                        // Validate package name format (should start with letter, contain dots, be at least 3 chars)
                        if (pkgName && pkgName.length > 3 && /^[a-zA-Z][a-zA-Z0-9_.]*$/.test(pkgName)) {
                            packageName = pkgName;
                        }
                    }
                    
                    // Pattern 2: Parse JSON and extract from ad0 entry
                    if (!packageName) {
                        try {
                            const parsed = JSON.parse(metaData);
                            if (Array.isArray(parsed) && parsed.length > 0) {
                                const firstArray = parsed[0];
                                if (Array.isArray(firstArray)) {
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
                                                                const pkg = pkgMatch[1];
                                                                if (/^[a-zA-Z][a-zA-Z0-9_.]*$/.test(pkg) && pkg.length > 3) {
                                                                    packageName = pkg;
                                                                }
                                                            }
                                                        }
                                                    } catch (e) { }
                                                }
                                                
                                                // Extract package directly from URL string
                                                if (!packageName) {
                                                    const pkgMatch = urlString.match(/id%3D([a-zA-Z0-9._]+)|[?&]id=([a-zA-Z0-9._]+)/);
                                                    if (pkgMatch) {
                                                        const pkg = pkgMatch[1] || pkgMatch[2];
                                                        if (pkg && /^[a-zA-Z][a-zA-Z0-9_.]*$/.test(pkg) && pkg.length > 3) {
                                                            packageName = pkg;
                                                        }
                                                    }
                                                }
                                            }
                                            break; // Found ad0, stop
                                        }
                                    }
                                }
                            }
                        } catch (e) { /* JSON parse failed */ }
                    }
                }
            }
        } catch (e) { /* Ignore */ }
        
        // Extract app name - EXACT SAME SELECTORS AND LOGIC as unified_agent
        // Check #portrait-landscape-phone first, then body
        const rootSelectors = ['#portrait-landscape-phone', 'body'];
        
        for (const rootSelector of rootSelectors) {
            const root = $(rootSelector).first();
            if (root.length === 0) continue;
            
            const appNameSelectors = [
                'a[data-asoch-targets*="ochAppName"]',
                'a[data-asoch-targets*="appname" i]',
                'a[data-asoch-targets*="rrappname" i]',
                'a[class*="short-app-name"]',
                '.short-app-name a'
            ];
            
            for (const selector of appNameSelectors) {
                const elements = root.find(selector);
                for (let i = 0; i < elements.length; i++) {
                    const el = $(elements[i]);
                    const rawName = el.text() || '';
                    const cleanedName = cleanAppName(rawName);
                    if (!cleanedName || cleanedName.toLowerCase() === blacklistName) continue;
                    
                    if (cleanedName && !appName) {
                        appName = cleanedName;
                        break;
                    }
                }
                if (appName) break;
            }
            
            // Fallback for app name
            if (!appName) {
                const textSels = ['[role="heading"]', 'div[class*="app-name"]', '.app-title'];
                for (const sel of textSels) {
                    const elements = root.find(sel);
                    for (let i = 0; i < elements.length; i++) {
                        const el = $(elements[i]);
                        const rawName = el.text() || '';
                        const cleanedName = cleanAppName(rawName);
                        if (cleanedName && cleanedName.toLowerCase() !== blacklistName) {
                            appName = cleanedName;
                            break;
                        }
                    }
                    if (appName) break;
                }
            }
            
            if (appName) break; // Found app name, stop searching
        }
        
        // Final fallback: extract from page title
        if (!appName || appName === 'NOT_FOUND' || appName === 'Ad Details') {
            try {
                const title = $('title').text() || '';
                if (title && !title.toLowerCase().includes('google ads')) {
                    const titleName = title.split(' - ')[0].split('|')[0].trim();
                    if (titleName) {
                        appName = cleanName(titleName);
                    }
                }
            } catch (e) { /* Ignore */ }
        }
        
        // Extract subtitle - EXACT SAME LOGIC as unified_agent (cS4Vcb-vnv8ic class)
        // Only extract if we have app name (text ad detection)
        const isTextAdForSubtitle = appName && appName !== 'NOT_FOUND' && appName !== 'Ad Details';
        
        if (isTextAdForSubtitle) {
            // Try extracting from iframes HTML content (if available in main HTML)
            // Then try main page
            for (const rootSelector of rootSelectors) {
                const root = $(rootSelector).first();
                if (root.length === 0) continue;
                
                // Try exact class first
                let subtitleEls = root.find('.cS4Vcb-vnv8ic');
                
                // If not found, try partial match
                if (subtitleEls.length === 0) {
                    subtitleEls = root.find('[class*="cS4Vcb"][class*="vnv8ic"]');
                }
                
                // If still not found, try any element with vnv8ic
                if (subtitleEls.length === 0) {
                    subtitleEls = root.find('[class*="vnv8ic"]');
                }
                
                const candidates = [];
                
                for (let i = 0; i < subtitleEls.length; i++) {
                    const el = $(subtitleEls[i]);
                    const text = el.text() || '';
                    const trimmedText = text.trim();
                    
                    if (trimmedText && trimmedText.length >= 3 && trimmedText.length <= 200) {
                        // Less strict validation - just avoid CSS-like content
                        if (!trimmedText.includes('{') && !trimmedText.includes('px') && 
                            !trimmedText.includes('height') && !trimmedText.includes('width')) {
                            candidates.push({ text: trimmedText, index: i });
                        }
                    }
                }
                
                // Return first valid subtitle (largest/earliest in DOM)
                if (candidates.length > 0) {
                    appSubtitle = candidates[0].text;
                    break;
                }
            }
        }
        
        // Apply cleanName to appName for final cleaning (same as unified_agent)
        if (appName && appName !== 'NOT_FOUND') {
            appName = cleanName(appName);
        }
        
        await page.close();
        
        return {
            success: true,
            blocked: false,
            html: htmlWithoutCSS,
            fullHTML: fullHTML,
            scripts: scripts,
            scriptsCount: scripts.length,
            // Extracted data - package name only (not full URL)
            packageName: packageName || 'NOT_FOUND',
            appName: appName || 'NOT_FOUND',
            appSubtitle: appSubtitle || 'NOT_FOUND'
        };
        
    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        try {
            await page.close();
        } catch (e) { /* Ignore */ }
        
        return {
            success: false,
            blocked: false,
            error: err.message,
            html: null
        };
    }
}

// ============================================
// MAIN EXECUTION
// ============================================
(async () => {
    console.log('🤖 HTML Parser Test Agent Starting...\n');
    console.log('🛡️  SAFETY FIRST - Maximum stealth enabled\n');
    console.log(`📋 Sheet: ${SHEET_NAME}`);
    console.log(`📝 Writing to: Column C (Play Store URL), D (App Name), F (Subtitle)`);
    if (HTML_OUTPUT_DIR) {
        console.log(`📁 HTML Output: ${HTML_OUTPUT_DIR} (optional)`);
    } else {
        console.log(`📁 HTML Output: Disabled (only writing to sheet)`);
    }
    console.log(`🔢 Max URLs to test: ${MAX_URLS_TO_TEST}\n`);
    
    // Create output directory only if HTML_OUTPUT_DIR is set
    if (HTML_OUTPUT_DIR && !fs.existsSync(HTML_OUTPUT_DIR)) {
        fs.mkdirSync(HTML_OUTPUT_DIR, { recursive: true });
        console.log(`✅ Created output directory: ${HTML_OUTPUT_DIR}\n`);
    }
    
    try {
        const sheets = await getGoogleSheetsClient();
        const toProcess = await getUrlData(sheets);
        
        if (toProcess.length === 0) {
            console.log('✨ No URLs found to process.');
            process.exit(0);
        }
        
        console.log(`🚀 Processing ${toProcess.length} URLs ONE BY ONE (safe mode)\n`);
        
        // Launch browser with maximum stealth
        const proxy = pickProxy();
        const launchArgs = [
            '--autoplay-policy=no-user-gesture-required',
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--no-zygote',
            '--single-process',
            '--disable-software-rasterizer',
            '--no-first-run',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process'
        ];
        
        if (proxy) {
            launchArgs.push(`--proxy-server=${proxy}`);
            console.log(`🌐 Using proxy: ${proxy}\n`);
        } else {
            console.log(`🌐 Running direct (no proxy)\n`);
        }
        
        const browser = await puppeteer.launch({
            headless: 'new',
            args: launchArgs,
            executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || null
        });
        
        // Process URLs ONE BY ONE (safest approach)
        for (let i = 0; i < toProcess.length; i++) {
            const item = toProcess[i];
            console.log(`\n${'='.repeat(80)}`);
            console.log(`📌 Processing URL ${i + 1}/${toProcess.length}`);
            console.log(`   Row: ${item.rowNumber}`);
            console.log(`   URL: ${item.url}`);
            console.log(`${'='.repeat(80)}`);
            
            let success = false;
            let result = null;
            
            // Retry logic
            for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
                result = await extractFullHTML(item.url, browser, attempt);
                
                if (result.success && !result.blocked) {
                    success = true;
                    break;
                }
                
                if (result.blocked) {
                    console.error(`  🛑 BLOCKED - Stopping to avoid further blocks`);
                    await browser.close();
                    process.exit(1);
                }
                
                if (attempt < MAX_RETRIES) {
                    console.log(`  🔄 Retrying in ${RETRY_DELAY / 1000}s...`);
                    await sleep(RETRY_DELAY);
                }
            }
            
            if (success && result.html) {
                // Extract data from HTML (already done in extractFullHTML)
                const packageName = result.packageName || 'NOT_FOUND';
                const appName = result.appName || 'NOT_FOUND';
                const appSubtitle = result.appSubtitle || 'NOT_FOUND';
                
                // Write ONLY to Columns C, D, F (required data only)
                // Column C: Full Play Store URL (https://play.google.com/store/apps/details?id=com.example.app)
                // Column D: App Name
                // Column F: App Subtitle
                await writeToSheet(sheets, item.rowIndex, packageName, appName, appSubtitle);
                
                // Construct full URL for display
                const fullStoreUrl = packageName && packageName !== 'NOT_FOUND' 
                    ? `https://play.google.com/store/apps/details?id=${packageName}` 
                    : 'NOT_FOUND';
                
                console.log(`\n✅ Success!`);
                console.log(`   📊 Scripts found: ${result.scriptsCount}`);
                console.log(`   📏 HTML size: ${(result.html.length / 1024).toFixed(2)} KB`);
                console.log(`   🔗 Play Store URL: ${fullStoreUrl}`);
                console.log(`   📦 Package Name: ${packageName}`);
                console.log(`   📱 App Name: ${appName}`);
                console.log(`   📝 Subtitle: ${appSubtitle.substring(0, 50)}...`);
                
                // Optional: Save HTML to file for inspection (only if HTML_OUTPUT_DIR is set)
                if (HTML_OUTPUT_DIR && HTML_OUTPUT_DIR !== '') {
                    const filepath = saveHTMLToFile(result.html, item.url, item.rowNumber);
                    if (filepath) {
                        console.log(`   📄 HTML saved to: ${filepath}`);
                    }
                }
            } else {
                console.error(`\n❌ Failed to extract HTML`);
                if (result && result.error) {
                    console.error(`   Error: ${result.error}`);
                }
                // Write NOT_FOUND to sheet even on failure
                if (item.needsMetadata) {
                    await writeToSheet(sheets, item.rowIndex, 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND');
                }
            }
            
            // SAFETY: Wait between URLs (prevent rate limiting)
            if (i < toProcess.length - 1) {
                console.log(`\n⏳ Waiting ${PAGE_LOAD_DELAY / 1000}s before next URL...`);
                await sleep(PAGE_LOAD_DELAY);
            }
        }
        
        await browser.close();
        
        console.log(`\n${'='.repeat(80)}`);
        console.log('🏁 Test Complete!');
        console.log(`✅ Data written to sheet: Columns C (Play Store URL), D (App Name), F (Subtitle)`);
        if (HTML_OUTPUT_DIR && HTML_OUTPUT_DIR !== '') {
            console.log(`📁 HTML files saved in: ${HTML_OUTPUT_DIR} (for inspection)`);
        }
        console.log(`${'='.repeat(80)}\n`);
        
    } catch (error) {
        console.error(`\n❌ Fatal error: ${error.message}`);
        console.error(error.stack);
        process.exit(1);
    }
})();
