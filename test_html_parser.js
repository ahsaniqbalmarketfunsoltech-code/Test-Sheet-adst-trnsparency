/**
 * HTML PARSER TEST AGENT
 * ======================
 * SAFETY FIRST - Maximum stealth and anti-detection
 * 
 * This agent:
 * - Connects to Google Sheets
 * - Processes URLs ONE BY ONE
 * - Scans ALL frames (Deep Inspect) to find hidden Play Store IDs
 * - Removes URL limits to process all empty rows
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
const HTML_OUTPUT_DIR = process.env.HTML_OUTPUT_DIR || '';
const MAX_URLS_TO_TEST = parseInt(process.env.MAX_URLS_TO_TEST) || 99999; // Process all

// SAFETY SETTINGS
// SAFETY SETTINGS
const PAGE_LOAD_DELAY = parseInt(process.env.PAGE_LOAD_DELAY) || 1000;
const WAIT_AFTER_LOAD = parseInt(process.env.WAIT_AFTER_LOAD) || 2500;
const MAX_WAIT_TIME = 90000;
const RETRY_DELAY = parseInt(process.env.RETRY_DELAY) || 10000;
const MAX_RETRIES = 2;

// Proxy settings
const PROXIES = process.env.PROXIES ? process.env.PROXIES.split(';').map(p => p.trim()).filter(Boolean) : [];

function pickProxy() {
    if (!PROXIES.length) return null;
    return PROXIES[Math.floor(Math.random() * PROXIES.length)];
}

const USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
];

const VIEWPORTS = [
    { width: 1920, height: 1080 },
    { width: 1366, height: 768 }
];

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ============================================
// GOOGLE SHEETS CLIENT
// ============================================
async function getGoogleSheetsClient() {
    let credentials;
    if (process.env.GOOGLE_CREDENTIALS) {
        try {
            credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS);
        } catch (err) {
            throw new Error('Error parsing GOOGLE_CREDENTIALS environment variable.');
        }
    } else if (fs.existsSync(CREDENTIALS_PATH)) {
        try {
            const content = fs.readFileSync(CREDENTIALS_PATH, 'utf8');
            credentials = JSON.parse(content);
        } catch (err) {
            throw err;
        }
    } else {
        throw new Error(`Google credentials not found.`);
    }

    const auth = new google.auth.GoogleAuth({
        credentials,
        scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const authClient = await auth.getClient();
    return google.sheets({ version: 'v4', auth: authClient });
}

async function getUrlData(sheets) {
    const toProcess = [];
    const range = `${SHEET_NAME}!A2:G100000`; // Scan up to 100,000 rows

    try {
        console.log(`📊 Reading whole sheet (${SHEET_NAME})...`);
        const response = await sheets.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: range,
        });

        const rows = response.data.values || [];
        console.log(`✅ Total rows found in sheet: ${rows.length + 1}`);

        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const actualRowIndex = i + 1;
            const url = row[1]?.trim() || '';
            const storeLink = row[2]?.trim() || '';

            if (!url) continue;

            // Criteria: Empty Column C
            if (!storeLink || storeLink.trim() === '') {
                // Determine if this is the first work item to log where we started
                if (toProcess.length === 0) {
                    console.log(`🔍 Work starts at Row ${actualRowIndex + 1}`);
                }

                toProcess.push({
                    url,
                    rowIndex: actualRowIndex,
                    rowNumber: actualRowIndex + 1,
                    needsMetadata: true
                });

                // Respect the cap if set via ENV, otherwise process ALL
                if (toProcess.length >= MAX_URLS_TO_TEST) break;
            }
        }

        console.log(`🎯 Found ${toProcess.length} empty rows needing data.`);
        console.log(`🚀 Starting sequential processing (one-by-one from Row ${toProcess[0]?.rowNumber || 'N/A'})...\n`);
        return toProcess;
    } catch (error) {
        console.error(`❌ Error loading sheet data: ${error.message}`);
        throw error;
    }
}

// ============================================
// CLEANING FUNCTIONS
// ============================================
function cleanName(name) {
    if (!name) return 'NOT_FOUND';
    let cleaned = name.trim();
    cleaned = cleaned.replace(/[\u200B-\u200D\uFEFF]/g, '');
    cleaned = cleaned.replace(/[a-zA-Z-]+\s*:\s*[^;]+;?/g, ' ');
    cleaned = cleaned.replace(/\d+px/g, ' ');
    cleaned = cleaned.replace(/\.[a-zA-Z][\w-]*/g, ' ');
    cleaned = cleaned.split('!@~!@~')[0];
    if (cleaned.includes('|')) cleaned = cleaned.split('|')[0];
    cleaned = cleaned.replace(/\s+/g, ' ').trim();

    // Safety check: ignore code-like strings
    if (cleaned.startsWith(',') || cleaned.startsWith('{') || cleaned.includes('{') || cleaned.includes('}') || cleaned.includes(';')) {
        return 'NOT_FOUND';
    }

    const black = cleaned.toLowerCase();
    const blacklist = ['ad details', 'google ads', 'sponsored', 'advertisement', 'transparency center', 'learn more'];
    if (blacklist.some(b => black === b || black.includes(b))) return 'NOT_FOUND';

    if (cleaned.length < 2 || cleaned.length > 80) return 'NOT_FOUND';
    if (/:\s*\d/.test(cleaned) || cleaned.includes('height') || cleaned.includes('width') || cleaned.includes('font-')) {
        return 'NOT_FOUND';
    }

    return cleaned || 'NOT_FOUND';
}

function removeCSSFromHTML(html) {
    return html.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
        .replace(/\s*style\s*=\s*["'][^"']*["']/gi, '')
        .replace(/<link[^>]*rel\s*=\s*["']stylesheet["'][^>]*>/gi, '');
}

async function writeToSheet(sheets, rowIndex, packageName, appName, appSubtitle) {
    const rowNum = rowIndex + 1;
    const data = [];
    const PLAY_STORE_URL_TEMPLATE = 'https://play.google.com/store/apps/details?id=';

    let storeLinkValue = 'NOT_FOUND';
    if (packageName && packageName !== 'NOT_FOUND') {
        storeLinkValue = `${PLAY_STORE_URL_TEMPLATE}${packageName}`;
    }
    data.push({ range: `${SHEET_NAME}!C${rowNum}`, values: [[storeLinkValue]] });
    data.push({ range: `${SHEET_NAME}!D${rowNum}`, values: [[appName || 'NOT_FOUND']] });
    data.push({ range: `${SHEET_NAME}!F${rowNum}`, values: [[appSubtitle || 'NOT_FOUND']] });

    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: { valueInputOption: 'RAW', data: data }
        });
        console.log(`  ✅ Wrote Row ${rowNum}: ${appName || 'NOT_FOUND'}`);
    } catch (error) {
        console.error(`  ❌ Write error:`, error.message);
    }
}

// ============================================
// DEEP EXTRACT HANDLER
// ============================================
async function extractFullHTML(url, browser, attempt = 1) {
    const page = await browser.newPage();
    try {
        const userAgent = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
        const viewport = VIEWPORTS[Math.floor(Math.random() * VIEWPORTS.length)];

        await page.setUserAgent(userAgent);
        await page.setViewport(viewport);

        // Block resources for speed
        await page.setRequestInterception(true);
        page.on('request', (request) => {
            const type = request.resourceType();
            if (['image', 'font', 'stylesheet'].includes(type)) request.abort();
            else request.continue();
        });

        console.log(`\n🌐 [Attempt ${attempt}] Loading: ${url.substring(0, 60)}...`);
        await page.goto(url, { waitUntil: 'networkidle2', timeout: MAX_WAIT_TIME });
        await sleep(WAIT_AFTER_LOAD);

        // =====================================================
        // DEEP INSPECTION - SCAN ALL FRAMES (WHOLE INSPECT)
        // =====================================================
        const frames = page.frames();
        let packageName = null;
        let appName = null;
        let appSubtitle = null;
        const blacklist = 'ad details';

        for (const frame of frames) {
            try {
                const res = await frame.evaluate((bl) => {
                    const d = { pkg: null, name: null, sub: null };

                    const isValidText = (t) => {
                        if (!t) return false;
                        const low = t.toLowerCase();
                        if (t.includes('{') || t.includes('}') || t.includes(';') || t.startsWith(',')) return false;
                        if (low.includes('ad details') || low.includes('google ads') || low.includes('sponsored') || low.includes('transparency center')) return false;

                        // Universal Button Filter (English, Spanish, French, German, Arabic, Russian, Asian languages)
                        const buttons = [
                            'install', 'open', 'download', 'play', 'get', 'more', 'visit site', 'learn more', // En
                            'instalar', 'abrir', 'descargar', 'jugar', 'obten', 'visitar', // Es/Pt
                            'installer', 'ouvrir', 'télécharger', 'jouer', // Fr
                            'installieren', 'öffnen', 'herunterladen', 'spielen', // De
                            'установить', 'открыть', 'скачать', 'играть', // Ru
                            'تثبيت', 'فتح', 'تحميل', 'لعب', 'زيارة', // Ar
                            'インストール', '開く', 'ダウンロード', 'プレイ', // Ja
                            '설치', '열기', '다운로드', '플레이', // Ko
                            '安装', '打开', '下载', '玩' // Zh
                        ];
                        if (buttons.some(b => low === b || low.includes(b) && low.length < 10)) return false;

                        // Filter Consent/Login screens
                        if (low.includes('before you continue') || low.includes('sign in') || low.includes('agree') || low.includes('cookie')) return false;
                        return t.length > 1 && t.length < 150;
                    };

                    // =========================================================
                    // PURE VISUAL SCAN (No Hidden "Inspect" Code)
                    // =========================================================
                    const adRoot = document.querySelector('#portrait-landscape-phone') ||
                        document.querySelector('.ad-creative') ||
                        document.body;

                    if (adRoot) {
                        // 1. App Name Selectors (Focus on Title/Heading)
                        const nameSels = [
                            'a[data-asoch-targets*="AppName"]',
                            '[data-asoch-targets*="AppName"]',
                            '.app-name',
                            '[class*="app-name"]',
                            '#creative-brand-name',
                            'div[role="heading"]',
                            '[aria-label="App Name"]',
                            'h3',
                            '.headline',
                            '#creative-headline'
                        ];

                        for (const s of nameSels) {
                            const els = adRoot.querySelectorAll(s);
                            for (const el of els) {
                                let t = el.innerText.trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
                                if (isValidText(t)) {
                                    if (!d.name) { d.name = t; break; }
                                }
                            }
                            if (d.name) break;
                        }

                        // 2. App Headline/Subtitle Selectors
                        const subSels = [
                            '[data-asoch-targets*="Headline"]',
                            '[data-asoch-targets*="Description"]',
                            'div[class*="description"]',
                            '.description',
                            'span[class*="description"]',
                            '.subtitle',
                            '#creative-description',
                            '.cS4Vcb-vnv8ic',
                            '[class*="vnv8ic"]'
                        ];

                        for (const s of subSels) {
                            const els = adRoot.querySelectorAll(s);
                            for (const el of els) {
                                let t = el.innerText.trim().replace(/[\u200B-\u200D\uFEFF]/g, '');
                                // Ensure subtitle is not identical to the name
                                if (isValidText(t) && t !== d.name) {
                                    d.sub = t; break;
                                }
                            }
                            if (d.sub) break;
                        }
                    }

                    return d;
                }, blacklist);

                if (res.pkg && !packageName) packageName = res.pkg;
                if (res.name && !appName) appName = res.name;
                if (res.sub && !appSubtitle) appSubtitle = res.sub;
                if (packageName && appName && appSubtitle) break;
            } catch (e) { }
        }

        const rawHTML = await page.content();
        const cleanHTML = removeCSSFromHTML(rawHTML);

        if (appName) appName = cleanName(appName);
        await page.close();

        return {
            success: true,
            html: cleanHTML,
            packageName: packageName || 'NOT_FOUND',
            appName: appName || 'NOT_FOUND',
            appSubtitle: appSubtitle || 'NOT_FOUND'
        };

    } catch (err) {
        console.error(`  ❌ Error: ${err.message}`);
        await page.close();
        return { success: false, blocked: false, error: err.message };
    }
}

// ============================================
// MAIN LOOP
// ============================================
(async () => {
    try {
        const sheets = await getGoogleSheetsClient();
        const toProcess = await getUrlData(sheets);

        if (toProcess.length === 0) {
            console.log('✨ No empty rows found.');
            process.exit(0);
        }

        const browser = await puppeteer.launch({
            headless: 'new',
            args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-web-security', '--disable-features=IsolateOrigins,site-per-process']
        });

        // -----------------------------------------------------
        // WARM UP PHASE: Handles "Consent" & Initial Cookies
        // -----------------------------------------------------
        console.log('🔥 Warming up browser to clear initial checks...');
        try {
            const page = await browser.newPage();
            // Visit the base domain to set cookies/storage usually
            await page.goto('https://adstransparency.google.com/', { waitUntil: 'networkidle2', timeout: 30000 });
            await sleep(2000);
            await page.close();
            console.log('✅ Warmup connection complete. Ready for data.');
        } catch (e) {
            console.log('⚠️ Warmup warning (proceeding anyway):', e.message);
        }

        for (let i = 0; i < toProcess.length; i++) {
            const item = toProcess[i];
            console.log(`\n📌 URL ${i + 1}/${toProcess.length} (Row ${item.rowNumber})`);

            let res = null;
            for (let att = 1; att <= MAX_RETRIES; att++) {
                res = await extractFullHTML(item.url, browser, att);
                if (res.success) break;
                await sleep(RETRY_DELAY);
            }

            if (res && res.success) {
                // ALWAYS write all 3 columns
                await writeToSheet(sheets, item.rowIndex, res.packageName, res.appName, res.appSubtitle);
            } else {
                // ON FAILURE: ALWAYS write NOT_FOUND to all 3 columns so the row is marked as processed
                console.log(`   ❌ Extraction failed, writing NOT_FOUND to columns C, D, F`);
                await writeToSheet(sheets, item.rowIndex, 'NOT_FOUND', 'NOT_FOUND', 'NOT_FOUND');
            }
            await sleep(PAGE_LOAD_DELAY);
        }

        await browser.close();
        console.log('\n🏁 Finished processing all requested rows.');
        console.log(`✅ Every empty row encountered was written to Column C, D, and F.`);
    } catch (error) {
        console.error(`\n❌ Fatal error: ${error.message}`);
        process.exit(1);
    }
})();
