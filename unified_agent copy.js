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
// LOAD AD TYPES CONFIGURATION
// ============================================
const AD_TYPES_CONFIG = JSON.parse(fs.readFileSync('./ad_types_config.json', 'utf8'));

// ============================================
// CONFIGURATION
// ============================================
const SPREADSHEET_ID = '1l4JpCcA1GSkta1CE77WxD_YCgePHI87K7NtMu1Sd4Q0';
const SHEET_NAME = process.env.SHEET_NAME || 'Test'; // Can be overridden via env var
const CREDENTIALS_PATH = './credentials.json';
const SHEET_BATCH_SIZE = parseInt(process.env.SHEET_BATCH_SIZE) || 2000; // Rows to load per batch
const CONCURRENT_PAGES = parseInt(process.env.CONCURRENT_PAGES) || 3; // Process 3 rows at a time (FIXED: always 3)
const MAX_WAIT_TIME = 60000;
const POST_CLICK_WAIT = 6000;
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
                // CRITICAL: Row indexing logic
                // - startRow = 1 (0-based index, represents row 2 in sheet)
                // - Range reads from row startRow+1 = row 2 in sheet
                // - actualRowIndex = startRow + i (0-based: 1 = row 2, 2 = row 3, etc.)
                // - When writing: rowNum = rowIndex + 1 (converts to 1-based for Google Sheets)
                //   Example: rowIndex 1 -> rowNum 2 (correct for row 2 in sheet)
                const actualRowIndex = startRow + i;
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
        // CRITICAL: rowIndex is 0-based (0=header, 1=first data row), rowNum is 1-based (Google Sheets)
        // rowIndex 1 = row 2 in sheet, rowIndex 2 = row 3 in sheet, etc.
        const rowNum = rowIndex + 1;

        // Helper function to check if value is valid (not dummy/placeholder data)
        const isValidValue = (val) => {
            if (!val || typeof val !== 'string') return false;
            const valUpper = val.toUpperCase();
            return !['SKIP', 'NOT_FOUND', 'BLOCKED', 'ERROR', 'EMPTY', ''].includes(valUpper) && val.trim().length > 0;
        };

        // Check if we have ANY valid data to write
        const hasValidData = isValidValue(advertiserName) || isValidValue(storeLink) || 
                            isValidValue(appName) || isValidValue(videoId) || 
                            isValidValue(appSubtitle) || isValidValue(imageUrl);

        // Only write if we have valid data (don't write dummy data)
        if (!hasValidData) {
            console.log(`  ⏭️  Row ${rowNum}: No valid data found, skipping write`);
            return; // Skip this row entirely
        }

            console.log(`  📝 Writing data to Row ${rowNum} (rowIndex=${rowIndex})`);

        // Only write advertiser name if it's valid
        if (isValidValue(advertiserName)) {
            data.push({ range: `'${SHEET_NAME}'!A${rowNum}`, values: [[advertiserName.trim()]] });
        }

        // Only write store link if it's valid
        if (isValidValue(storeLink)) {
            data.push({ range: `'${SHEET_NAME}'!C${rowNum}`, values: [[storeLink.trim()]] });
        }

        // Only write app name if it's valid
        if (isValidValue(appName)) {
            data.push({ range: `'${SHEET_NAME}'!D${rowNum}`, values: [[appName.trim()]] });
        }

        // Only write video ID if it's valid
        if (isValidValue(videoId)) {
            data.push({ range: `'${SHEET_NAME}'!E${rowNum}`, values: [[videoId.trim()]] });
        }

        // Only write subtitle if it's valid
        if (isValidValue(appSubtitle)) {
            data.push({ range: `'${SHEET_NAME}'!F${rowNum}`, values: [[appSubtitle.trim()]] });
        }

        // Only write image URL if it's valid
        if (isValidValue(imageUrl)) {
            data.push({ range: `'${SHEET_NAME}'!G${rowNum}`, values: [[imageUrl.trim()]] });
        }

        // Only write timestamp if we wrote at least one valid field
        if (data.length > 0) {
        const timestamp = new Date().toLocaleString('en-PK', { timeZone: 'Asia/Karachi' });
        data.push({ range: `'${SHEET_NAME}'!M${rowNum}`, values: [[timestamp]] });
        }
    });

    if (data.length === 0) {
        console.log(`  ⏭️  No valid data to write for this batch`);
        return;
    }

    try {
        await sheets.spreadsheets.values.batchUpdate({
            spreadsheetId: SPREADSHEET_ID,
            resource: { valueInputOption: 'RAW', data: data }
        });
        console.log(`  ✅ Wrote ${data.filter(d => !d.range.includes('M')).length} valid fields to sheet`);
    } catch (error) {
        console.error(`  ❌ Write error:`, error.message);
    }
}

// ============================================
// AD TYPE DETECTION & EXTRACTION HELPERS
// ============================================
async function detectAdType(page) {
    // Wait longer for elements to load, especially iframes
    await sleep(2000);
    
    // First, check iframes - ads are often in iframes
    const frames = page.frames();
    console.log(`  🔍 Checking ${frames.length} frames for ad elements...`);
    
    // Check main page and all frames
    const allResults = await Promise.all([
        checkPageForAdType(page, 'main'),
        ...frames.map((frame, idx) => checkPageForAdType(page, `frame-${idx}`, frame))
    ]);
    
    // Return first non-unknown result, or unknown if all failed
    for (const result of allResults) {
        if (result.type !== 'unknown') {
            console.log(`  ✅ Found ${result.type} in ${result.location}`);
            return result.type;
        }
    }
    
    // If still unknown, log comprehensive debug info
    const debugInfo = await page.evaluate(() => {
        const info = {
            mainPage: {
                hasLandscapeView: !!document.querySelector('#landscape-view, .landscape-view'),
                hasPortraitView: !!document.querySelector('#portrait-view, .portrait-view'),
                hasLandscapeImage: !!document.querySelector('#landscape-image'),
                hasPortraitImage: !!document.querySelector('#portrait-image'),
                textAdElements: document.querySelectorAll('.cS4Vcb-vnv8ic, div.cS4Vcb-vnv8ic, [class*="cS4Vcb"], [class*="vmv8lc"]').length,
                allImages: document.querySelectorAll('img').length,
                allDivs: document.querySelectorAll('div').length,
                sampleClasses: Array.from(document.querySelectorAll('div[class*="c"]')).slice(0, 10).map(d => d.className).filter(c => c),
                sampleIds: Array.from(document.querySelectorAll('[id*="landscape"], [id*="portrait"]')).slice(0, 5).map(el => el.id)
            }
        };
        return info;
    });
    
    console.log(`  🔍 Main Page Debug:`);
    console.log(`     - Image containers: landscape=${debugInfo.mainPage.hasLandscapeView}, portrait=${debugInfo.mainPage.hasPortraitView}`);
    console.log(`     - Image elements: landscape=${debugInfo.mainPage.hasLandscapeImage}, portrait=${debugInfo.mainPage.hasPortraitImage}`);
    console.log(`     - Text ad elements: ${debugInfo.mainPage.textAdElements}`);
    console.log(`     - Total images: ${debugInfo.mainPage.allImages}, Total divs: ${debugInfo.mainPage.allDivs}`);
    if (debugInfo.mainPage.sampleClasses.length > 0) {
        console.log(`     - Sample classes: ${debugInfo.mainPage.sampleClasses.slice(0, 3).join(', ')}`);
    }
    if (debugInfo.mainPage.sampleIds.length > 0) {
        console.log(`     - Sample IDs: ${debugInfo.mainPage.sampleIds.join(', ')}`);
    }
    
    return 'unknown';
}

async function checkPageForAdType(page, location, frame = null) {
    const context = frame || page;
    
    try {
        const result = await context.evaluate(() => {
            // Check for Image Ad first
            const imageViewSelectors = ['#landscape-view', '#portrait-view', '.landscape-view', '.portrait-view'];
            for (const sel of imageViewSelectors) {
                const imageView = document.querySelector(sel);
                if (imageView) {
                    const imageElement = imageView.querySelector('#landscape-image, #portrait-image, img#landscape-image, img#portrait-image, img');
                    if (imageElement) {
                        const rect = imageElement.getBoundingClientRect();
                        if (rect.width > 0 && rect.height > 0) {
                            return { type: 'image_ad', found: sel };
                        }
                    }
                }
            }
            
            // Check for direct image elements
            const directImageSelectors = ['#landscape-image', '#portrait-image', 'img#landscape-image', 'img#portrait-image'];
            for (const sel of directImageSelectors) {
                const img = document.querySelector(sel);
                if (img) {
                    const rect = img.getBoundingClientRect();
                    if (rect.width > 0 && rect.height > 0 && rect.width > 50 && rect.height > 50) {
                        return { type: 'image_ad', found: sel };
                    }
                }
            }
            
            // Check for Text Ad
            const textAdSelectors = [
                '.cS4Vcb-vnv8ic',
                'div.cS4Vcb-vnv8ic',
                '[class*="cS4Vcb"]',
                '[class*="vmv8lc"]',
                '[class*="c54Vcb"]'
            ];
            
            for (const sel of textAdSelectors) {
                const elements = document.querySelectorAll(sel);
                if (elements.length > 0) {
                    for (const el of elements) {
                        const rect = el.getBoundingClientRect();
                        const text = (el.innerText || el.textContent || '').trim();
                        if (rect.width > 0 && rect.height > 0 && text.length > 3) {
                            return { type: 'text_ad', found: sel };
                        }
                    }
                }
            }
            
            // Fallback: Check for any div with text that might be a text ad
            const allDivs = document.querySelectorAll('div[class*="cS4Vcb"], div[class*="vmv8lc"], div[class*="c54Vcb"]');
            for (const div of allDivs) {
                const rect = div.getBoundingClientRect();
                const text = (div.innerText || div.textContent || '').trim();
                if (rect.width > 0 && rect.height > 0 && text.length > 5 && text.length < 200) {
                    if (!div.closest('nav') && !div.closest('header') && !div.closest('footer')) {
                        return { type: 'text_ad', found: 'fallback-div' };
                    }
                }
            }
            
            return { type: 'unknown', found: null };
        });
        
        return { ...result, location };
    } catch (e) {
        // Frame might be cross-origin, skip it
        return { type: 'unknown', location, error: e.message };
    }
}

async function extractImageAdData(page, config) {
    const result = {
        imageUrl: 'NOT_FOUND',
        appName: 'NOT_FOUND',
        appSubtitle: 'NOT_FOUND',
        storeLink: 'NOT_FOUND'
    };
    
    try {
        // Check main page and all frames
        const frames = page.frames();
        const contexts = [{ name: 'main', frame: null }, ...frames.map((f, i) => ({ name: `frame-${i}`, frame: f }))];
        
        for (const ctx of contexts) {
            const context = ctx.frame || page;
            
            try {
                // Step 1: Find visible ad container (try multiple selectors)
                let adViewContainer = await context.$('#landscape-view, #portrait-view');
                if (!adViewContainer) {
                    adViewContainer = await context.$('.landscape-view, .portrait-view');
                }
                if (!adViewContainer) {
                    // Try finding container by looking for image element first
                    const containerSelector = await context.evaluate(() => {
                        const img = document.querySelector('#landscape-image, #portrait-image');
                        if (img) {
                            const container = img.closest('#landscape-view, #portrait-view, .landscape-view, .portrait-view');
                            if (container) {
                                // Return a unique selector for this container
                                if (container.id) return `#${container.id}`;
                                if (container.className) {
                                    const classes = container.className.split(' ').filter(c => c.includes('landscape') || c.includes('portrait'));
                                    if (classes.length > 0) return `.${classes[0]}`;
                                }
                            }
                        }
                        return null;
                    });
                    if (containerSelector) {
                        adViewContainer = await context.$(containerSelector);
                    }
                }
                if (!adViewContainer) {
                    continue; // Try next frame
                }
                
                console.log(`  ✅ Found image ad container in ${ctx.name}`);
        
                // Step 2: Find image element inside container (try multiple selectors)
                let imageElement = await adViewContainer.$('#landscape-image, #portrait-image');
                if (!imageElement) {
                    imageElement = await adViewContainer.$('img#landscape-image, img#portrait-image');
                }
                if (!imageElement) {
                    imageElement = await adViewContainer.$('img');
                }
                if (!imageElement) {
                    continue; // Try next frame
                }
                
                // Step 3: Hover on image element
                await imageElement.hover();
                await sleep(500);
                
                // Step 4: Extract all data from hovered container
                const extractedData = await context.evaluate((config) => {
            const data = {
                imageUrl: null,
                appName: null,
                appHeadline: null,
                packageName: null
            };
            
            // Find container
            const container = document.querySelector('#landscape-view, #portrait-view, .landscape-view, .portrait-view');
            if (!container) return data;
            
            // Extract Image URL
            const img = container.querySelector('#landscape-image, #portrait-image, img#landscape-image, img#portrait-image');
            if (img && img.src) {
                data.imageUrl = img.src.trim();
            }
            
            // Extract App Name
            const appNameEl = container.querySelector('#landscape-app-title, #portrait-app-title, .landscape-app-title, .portrait-app-title, span.landscape-app-title, span.portrait-app-title');
            if (appNameEl) {
                data.appName = (appNameEl.innerText || appNameEl.textContent || '').trim();
            }
            
            // Extract App Headline
            const headlineEl = container.querySelector('#landscape-app-text, #portrait-app-text, .landscape-app-text, .portrait-app-text, div.landscape-app-text, div.portrait-app-text');
            if (headlineEl) {
                data.appHeadline = (headlineEl.innerText || headlineEl.textContent || '').trim();
            }
            
            // Extract Package Name from JavaScript data
            const pageContent = document.documentElement.innerHTML;
            const patterns = [
                /appId\s*[:=]\s*['"](com\.[a-z0-9_]+(?:\.[a-z0-9_]+)+)['"]/i,
                /adData[^}]*appId\s*[:=]\s*['"](com\.[a-z0-9_]+(?:\.[a-z0-9_]+)+)['"]/i
            ];
            
            for (const pattern of patterns) {
                const match = pageContent.match(pattern);
                if (match && match[1]) {
                    data.packageName = match[1];
                    break;
                }
            }
            
                    return data;
                }, config);
                
                if (extractedData.imageUrl) result.imageUrl = extractedData.imageUrl;
                if (extractedData.appName) result.appName = extractedData.appName;
                if (extractedData.appHeadline) result.appSubtitle = extractedData.appHeadline;
                if (extractedData.packageName) {
                    result.storeLink = `https://play.google.com/store/apps/details?id=${extractedData.packageName}`;
                }
                
                // If we got any data, return it
                if (result.imageUrl !== 'NOT_FOUND' || result.appName !== 'NOT_FOUND' || result.storeLink !== 'NOT_FOUND') {
                    console.log(`  🖼️ Image Ad (${ctx.name}) - URL: ${result.imageUrl.substring(0, 50)}... | Name: ${result.appName} | Headline: ${result.appSubtitle}`);
                    return result;
                }
            } catch (e) {
                // Cross-origin frame or other error, continue to next
                continue;
            }
        }
        
        console.log(`  ⚠️ Image ad container not found in any frame`);
        
    } catch (e) {
        console.log(`  ⚠️ Image ad extraction failed: ${e.message}`);
    }
    
    return result;
}

async function extractTextAdData(page, config) {
    const result = {
        appName: 'NOT_FOUND',
        appSubtitle: 'NOT_FOUND',
        storeLink: 'NOT_FOUND'
    };
    
    try {
        // Check main page and all frames
        const frames = page.frames();
        const contexts = [{ name: 'main', frame: null }, ...frames.map((f, i) => ({ name: `frame-${i}`, frame: f }))];
        
        for (const ctx of contexts) {
            const context = ctx.frame || page;
            
            try {
                // Step 1: Find text ad element (try multiple selectors)
                let textAdElement = await context.$('.cS4Vcb-vnv8ic, div.cS4Vcb-vnv8ic');
                if (!textAdElement) {
                    textAdElement = await context.$('[class*="cS4Vcb"]');
                }
                if (!textAdElement) {
                    textAdElement = await context.$('[class*="vmv8lc"]');
                }
                if (!textAdElement) {
                    textAdElement = await context.$('[class*="c54Vcb"]');
                }
                if (!textAdElement) {
                    // Last resort: try role="link" but only if it has text
                    const links = await context.$$('[role="link"]');
                    for (const link of links) {
                        const text = await link.evaluate(el => (el.innerText || el.textContent || '').trim());
                        if (text && text.length > 5 && text.length < 200) {
                            textAdElement = link;
                            break;
                        }
                    }
                }
                if (!textAdElement) {
                    continue; // Try next frame
                }
                
                console.log(`  ✅ Found text ad element in ${ctx.name}`);
                
                // Step 2: Scroll element into view and try to hover
                try {
                    // Scroll element into view first
                    await textAdElement.evaluate(el => el.scrollIntoView({ behavior: 'smooth', block: 'center' }));
                    await sleep(300);
                    
                    // Try to find a hoverable parent element (div, a, etc.)
                    let hoverableElement = textAdElement;
                    try {
                        const parentElement = await textAdElement.evaluateHandle(el => {
                            // Try to find a parent that's hoverable (div, a, span with pointer events)
                            let parent = el.parentElement;
                            while (parent && parent !== document.body) {
                                const tag = parent.tagName.toLowerCase();
                                if (tag === 'div' || tag === 'a' || tag === 'span' || tag === 'article') {
                                    return parent;
                                }
                                parent = parent.parentElement;
                            }
                            return el; // Fallback to original element
                        });
                        hoverableElement = parentElement.asElement();
                        if (!hoverableElement) hoverableElement = textAdElement;
                    } catch (e) {
                        // If that fails, use original element
                        hoverableElement = textAdElement;
                    }
                    
                    // Try hover using JavaScript event instead of Puppeteer's hover
                    await hoverableElement.evaluate(el => {
                        const event = new MouseEvent('mouseenter', {
                            view: window,
                            bubbles: true,
                            cancelable: true
                        });
                        el.dispatchEvent(event);
                    });
                    await sleep(500);
                } catch (hoverError) {
                    console.log(`  ⚠️ Hover failed in ${ctx.name}, extracting without hover: ${hoverError.message}`);
                    // Continue without hover - we'll extract anyway
                }
                
                // Step 3: Get element info for extraction
                const elementInfo = await textAdElement.evaluate(el => {
                    // Find the parent container that likely contains all ad data
                    let container = el.closest('[class*="ad"], [class*="creative"], [class*="preview"], [role="article"], [role="link"]');
                    if (!container) container = el.parentElement;
                    return {
                        containerSelector: container ? (container.id ? `#${container.id}` : container.className ? `.${container.className.split(' ')[0]}` : null) : null,
                        headlineText: (el.innerText || el.textContent || '').trim(),
                        elementText: (el.innerText || el.textContent || '').trim()
                    };
                });
                
                // Step 4: Extract all data - use the element we already found
                const extractedData = await textAdElement.evaluate((el) => {
                    const data = {
                        appName: null,
                        appHeadline: null,
                        packageName: null
                    };
                    
                    // Use the element we already found (el parameter)
                    const targetElement = el;
                    
                    if (!targetElement) return data;
                    
                    // Find the container around this element
                    let container = targetElement.closest('[class*="ad"], [class*="creative"], [class*="preview"], [role="article"]');
                    if (!container) container = targetElement.parentElement;
                    if (!container) container = document.body;
                    
                    // Extract App Name - try multiple strategies
                    // Strategy 1: Look for spans ABOVE the headline (app name is usually above headline)
                    const headlineRect = targetElement.getBoundingClientRect();
                    const spanElements = container.querySelectorAll('span');
                    let foundAppName = false;
                    
                    for (const span of spanElements) {
                        const text = (span.innerText || span.textContent || '').trim();
                        const rect = span.getBoundingClientRect();
                        
                        // Check if span is above the headline and visible
                        if (rect.top < headlineRect.top && rect.width > 0 && rect.height > 0 && 
                            text && text.length > 3 && text.length < 150 &&
                            !text.toLowerCase().includes('install') &&
                            !text.toLowerCase().includes('download') &&
                            !text.toLowerCase().includes('get it') &&
                            !text.toLowerCase().includes('play store') &&
                            !text.toLowerCase().includes('advertisement')) {
                            // Check if it's reasonably close (within 100px above)
                            const distance = headlineRect.top - rect.bottom;
                            if (distance > 0 && distance < 100) {
                                data.appName = text;
                                foundAppName = true;
                                break;
                            }
                        }
                    }
                    
                    // Strategy 2: Look for parent element's first child text (app name might be in parent)
                    if (!foundAppName && targetElement.parentElement) {
                        const parent = targetElement.parentElement;
                        const parentText = (parent.innerText || parent.textContent || '').trim();
                        // If parent has multiple text nodes, first might be app name
                        const children = Array.from(parent.children);
                        for (const child of children) {
                            if (child === targetElement) break; // Stop at headline element
                            const childText = (child.innerText || child.textContent || '').trim();
                            if (childText && childText.length > 3 && childText.length < 150 &&
                                !childText.toLowerCase().includes('install') &&
                                !childText.toLowerCase().includes('download')) {
                                data.appName = childText;
                                foundAppName = true;
                                break;
                            }
                        }
                    }
                    
                    // Strategy 3: Look for spans near headline (within container, any position)
                    if (!foundAppName) {
                        for (const span of spanElements) {
                            const text = (span.innerText || span.textContent || '').trim();
                            if (text && text.length > 5 && text.length < 150 && 
                                text !== headlineText && // Don't use headline as app name
                                !text.toLowerCase().includes('install') &&
                                !text.toLowerCase().includes('download') &&
                                !text.toLowerCase().includes('get it') &&
                                !text.toLowerCase().includes('play store')) {
                                const rect = span.getBoundingClientRect();
                                if (rect.width > 0 && rect.height > 0 && rect.top > 0 && rect.left > 0) {
                                    const distance = Math.abs(rect.top - headlineRect.top);
                                    if (distance < 150) { // Within 150px vertically
                                        data.appName = text;
                                        foundAppName = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    
                    // Extract App Headline from the hovered element itself
                    const headlineText = (targetElement.innerText || targetElement.textContent || '').trim();
                    if (headlineText && headlineText.length > 3 && headlineText.length < 200) {
                        data.appHeadline = headlineText;
                    }
                    
                    // Don't use headline as app name - they should be separate
                    // Only use headline parts if we really can't find an app name elsewhere
                    // and the headline looks like it might contain an app name (has separator)
                    if (!data.appName && headlineText && headlineText.includes('|')) {
                        // Only if headline has separator, try first part
                        const parts = headlineText.split(/[|\-–—]/).map(p => p.trim()).filter(p => p.length > 3);
                        if (parts.length > 1 && parts[0].length > 5) {
                            data.appName = parts[0]; // First part might be app name
                            data.appHeadline = parts.slice(1).join(' | '); // Rest is headline
                        }
                    }
                    // If still no app name, don't use headline - leave it as null
                    
                    // Extract Package Name - scope to current ad container to avoid wrong packages
                    // First, try to find package in the ad container's HTML (most reliable)
                    let containerHTML = container.innerHTML;
                    if (!containerHTML || containerHTML.length < 100) {
                        // If container HTML is too small, use frame HTML but be more selective
                        containerHTML = document.documentElement.innerHTML;
                    }
                    
                    // Pattern 1: Look for Play Store links in container (most reliable)
                    const playStoreMatch = containerHTML.match(/play\.google\.com\/store\/apps\/details\?id=([a-z0-9_.]+)/i);
                    if (playStoreMatch && playStoreMatch[1]) {
                        const pkg = playStoreMatch[1];
                        if (pkg.startsWith('com.') && pkg.split('.').length >= 3 &&
                            !pkg.includes('google') && !pkg.includes('android') &&
                            pkg.length > 10) {
                            data.packageName = pkg;
                        }
                    }
                    
                    // Pattern 2: Look for data-asoch-meta with package (scoped to container)
                    if (!data.packageName) {
                        const metaElements = container.querySelectorAll('meta[data-asoch-meta], *[data-asoch-meta]');
                        for (const meta of metaElements) {
                            const metaValue = meta.getAttribute('data-asoch-meta') || '';
                            if (metaValue.includes('play.google.com')) {
                                const match = metaValue.match(/id=([a-zA-Z0-9._]+)/);
                                if (match && match[1]) {
                                    const pkg = match[1];
                                    if (pkg.startsWith('com.') && pkg.split('.').length >= 3 &&
                                        !pkg.includes('google') && !pkg.includes('android') &&
                                        pkg.length > 10) {
                                        data.packageName = pkg;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    
                    // Pattern 3: Look for appId in JavaScript near the container (last resort)
                    if (!data.packageName) {
                        // Try to find script tags near the container
                        const scripts = document.querySelectorAll('script');
                        for (const script of scripts) {
                            const scriptText = script.textContent || script.innerHTML || '';
                            // Look for appId patterns near ad-related keywords
                            if (scriptText.includes('appId') || scriptText.includes('play.google.com')) {
                                const patterns = [
                                    /appId\s*[:=]\s*['"](com\.[a-z0-9_]+(?:\.[a-z0-9_]+)+)['"]/i,
                                    /play\.google\.com\/store\/apps\/details\?id=([a-z0-9_.]+)/i
                                ];
                                for (const pattern of patterns) {
                                    const match = scriptText.match(pattern);
                                    if (match && match[1]) {
                                        const pkg = match[1];
                                        if (pkg.startsWith('com.') && pkg.split('.').length >= 3 &&
                                            !pkg.includes('google') && !pkg.includes('android') &&
                                            !pkg.includes('example') && pkg.length > 10) {
                                            data.packageName = pkg;
                                            break;
                                        }
                                    }
                                }
                                if (data.packageName) break;
                            }
                        }
                    }
                    
                    return data;
                });
                
                // Debug: log what we extracted
                console.log(`  🔍 Extracted from ${ctx.name}: appName="${extractedData.appName}", headline="${extractedData.appHeadline}", package="${extractedData.packageName}"`);
                
                // Update result with extracted data
                const appName = extractedData.appName ? extractedData.appName.trim() : null;
                const headline = extractedData.appHeadline ? extractedData.appHeadline.trim() : null;
                
                // Set app name if we found one (even if same as headline - some ads are like that)
                if (appName) {
                    result.appName = appName;
                }
                // Set headline/subtitle
                if (headline) {
                    result.appSubtitle = headline;
                }
                
                if (extractedData.packageName && extractedData.packageName.trim()) {
                    result.storeLink = `https://play.google.com/store/apps/details?id=${extractedData.packageName.trim()}`;
                }
                
                // If we got app name or headline but no package, try searching ALL frames HTML
                // BUT prioritize the frame where we found the ad element
                if ((result.appName !== 'NOT_FOUND' || result.appSubtitle !== 'NOT_FOUND') && result.storeLink === 'NOT_FOUND') {
                    try {
                        // Search all frames for package name, but prioritize current frame
                        const frames = page.frames();
                        let foundPackage = null;
                        const commonWrongPackages = ['com.audio.mp3player.video.musicplayer']; // Known wrong packages to avoid
                        
                        // First, try the frame where we found the ad (most reliable)
                        if (ctx.frame) {
                            try {
                                const currentFramePackage = await ctx.frame.evaluate(() => {
                                    // Look for Play Store links in data-asoch-meta (most reliable)
                                    const metaElements = document.querySelectorAll('meta[data-asoch-meta], *[data-asoch-meta]');
                                    for (const meta of metaElements) {
                                        const metaValue = meta.getAttribute('data-asoch-meta') || '';
                                        if (metaValue.includes('play.google.com')) {
                                            const match = metaValue.match(/id=([a-zA-Z0-9._]+)/);
                                            if (match && match[1]) {
                                                return match[1];
                                            }
                                        }
                                    }
                                    
                                    // Fallback: look for Play Store links in HTML
                                    const pageContent = document.documentElement.innerHTML;
                                    const playStoreMatches = pageContent.match(/play\.google\.com\/store\/apps\/details\?id=([a-z0-9_.]+)/gi);
                                    if (playStoreMatches && playStoreMatches.length > 0) {
                                        const pkg = playStoreMatches[0].match(/id=([a-z0-9_.]+)/i);
                                        if (pkg && pkg[1]) return pkg[1];
                                    }
                                    
                                    return null;
                                });
                                
                                if (currentFramePackage && !commonWrongPackages.includes(currentFramePackage)) {
                                    foundPackage = currentFramePackage;
                                    console.log(`  ✅ Found package from current frame (${ctx.name}): ${currentFramePackage}`);
                                }
                            } catch (e) {
                                // Cross-origin, continue
                            }
                        }
                        
                        // If not found in current frame, search other frames
                        if (!foundPackage) {
                            for (const frame of frames) {
                                // Skip the frame we already checked
                                if (ctx.frame && frame === ctx.frame) continue;
                                
                                try {
                                    const framePackage = await frame.evaluate(() => {
                                        const pageContent = document.documentElement.innerHTML;
                                        
                                        // Pattern 1: Direct Play Store links (most reliable)
                                        const playStoreMatches = pageContent.match(/play\.google\.com\/store\/apps\/details\?id=([a-z0-9_.]+)/gi);
                                        if (playStoreMatches && playStoreMatches.length > 0) {
                                            // Use first match
                                            const pkg = playStoreMatches[0].match(/id=([a-z0-9_.]+)/i);
                                            if (pkg && pkg[1]) return pkg[1];
                                        }
                                        
                                        // Pattern 2: data-asoch-meta (very reliable)
                                        const metaElements = document.querySelectorAll('meta[data-asoch-meta], *[data-asoch-meta]');
                                        for (const meta of metaElements) {
                                            const metaValue = meta.getAttribute('data-asoch-meta') || '';
                                            if (metaValue.includes('play.google.com')) {
                                                const match = metaValue.match(/id=([a-zA-Z0-9._]+)/);
                                                if (match && match[1]) return match[1];
                                            }
                                        }
                                        
                                        return null;
                                    });
                                    
                                    if (framePackage && !commonWrongPackages.includes(framePackage)) {
                                        foundPackage = framePackage;
                                        console.log(`  ✅ Found package from ${frame.url().substring(0, 50)}: ${framePackage}`);
                                        break;
                                    }
                                } catch (e) {
                                    // Cross-origin frame, skip
                                    continue;
                                }
                            }
                        }
                        
                        // Also search main page
                        if (!foundPackage) {
                            const mainPagePackage = await page.evaluate(() => {
                                const pageContent = document.documentElement.innerHTML;
                                const playStoreMatches = pageContent.match(/play\.google\.com\/store\/apps\/details\?id=([a-z0-9_.]+)/gi);
                                if (playStoreMatches) {
                                    for (const match of playStoreMatches) {
                                        const pkg = match.match(/id=([a-z0-9_.]+)/i);
                                        if (pkg && pkg[1] && pkg[1].startsWith('com.') && pkg[1].split('.').length >= 3 &&
                                            !pkg[1].includes('google') && !pkg[1].includes('android') && pkg[1].length > 10) {
                                            return pkg[1];
                                        }
                                    }
                                }
                                return null;
                            });
                            
                            if (mainPagePackage) {
                                foundPackage = mainPagePackage;
                                console.log(`  ✅ Found package from main page: ${mainPagePackage}`);
                            }
                        }
                        
                        if (foundPackage) {
                            result.storeLink = `https://play.google.com/store/apps/details?id=${foundPackage}`;
                        }
                    } catch (e) {
                        console.log(`  ⚠️ Package search error: ${e.message}`);
                    }
                }
                
                // If we got any data, return it
                if (result.appName !== 'NOT_FOUND' || result.storeLink !== 'NOT_FOUND' || result.appSubtitle !== 'NOT_FOUND') {
                    console.log(`  ✅ Text Ad (${ctx.name}) - Name: ${result.appName} | Headline: ${result.appSubtitle} | Link: ${result.storeLink}`);
                    return result;
                } else {
                    console.log(`  ⚠️ No data extracted from ${ctx.name}, trying next frame...`);
                }
            } catch (e) {
                // Cross-origin frame or other error, log and continue
                console.log(`  ⚠️ Error in ${ctx.name}: ${e.message}`);
                continue;
            }
        }
        
        console.log(`  ⚠️ Text ad element found but no data extracted from any frame`);
        
    } catch (e) {
        console.log(`  ⚠️ Text ad extraction failed: ${e.message}`);
    }
    
    return result;
}

// ============================================
// UNIFIED EXTRACTION - ONE VISIT PER URL
// Database-driven extraction based on ad type
// ============================================
async function extractAllInOneVisit(url, browser, needsMetadata, needsVideoId, existingStoreLink) {
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
        
        // FIXED: Don't split on | - keep full name
        // Only split if it's clearly a separator (with spaces around it)
        if (cleaned.includes(' | ') || cleaned.match(/\s+\|\s+/)) {
            const parts = cleaned.split(/\s*\|\s*/).map(p => p.trim()).filter(p => p.length > 2);
            if (parts.length > 1 && parts[0].length > 5) {
                cleaned = parts[0]; // Use first part if substantial
            }
            // Otherwise keep full text
        }

        // Normalize whitespace
        cleaned = cleaned.replace(/\s+/g, ' ').trim();

        // Blacklist for app names (Global) - only reject if exact match or very short phrases
        const blacklistNames = [
            'ad details', 'google ads', 'transparency center', 'about this ad',
            'privacy policy', 'terms of service', 'install now', 'download',
            'play store', 'app store', 'advertisement', 'sponsored',
            'learn more', 'blocked'
        ];
        // Only reject if it's an exact match or a very short phrase (1-2 words)
        const wordCount = cleaned.split(/\s+/).length;
        if (wordCount <= 2 && blacklistNames.some(n => cleaned.toLowerCase() === n)) return 'NOT_FOUND';
        // For longer phrases, only reject if it contains exact blacklist phrase
        if (wordCount > 2 && blacklistNames.some(n => cleaned.toLowerCase().includes(n) && n.length > 5)) return 'NOT_FOUND';

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
        await sleep(baseWait);

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
        // PHASE 1: AD TYPE DETECTION & EXTRACTION
        // =====================================================
        if (needsMetadata) {
            console.log(`  📊 Detecting ad type...`);
            
            // Get advertiser name first
            const advertiserName = await page.evaluate(() => {
                const selectors = ['.advertiser-name', '.advertiser-name-container', 'h1', '.creative-details-page-header-text', '.ad-details-heading'];
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el) {
                        const text = el.innerText.trim();
                        if (text && text.length > 2) return text;
                    }
                }
                return 'NOT_FOUND';
            });
            result.advertiserName = advertiserName;
            
            // Detect ad type
            const adType = await detectAdType(page);
            console.log(`  🔍 Detected ad type: ${adType}`);
            
            // Extract data based on ad type
            if (adType === 'image_ad') {
                const imageAdData = await extractImageAdData(page, AD_TYPES_CONFIG.image_ad);
                if (imageAdData.imageUrl !== 'NOT_FOUND') result.imageUrl = imageAdData.imageUrl;
                if (imageAdData.appName !== 'NOT_FOUND') result.appName = cleanName(imageAdData.appName);
                if (imageAdData.appSubtitle !== 'NOT_FOUND') result.appSubtitle = imageAdData.appSubtitle;
                if (imageAdData.storeLink !== 'NOT_FOUND') result.storeLink = imageAdData.storeLink;
            } else if (adType === 'text_ad') {
                const textAdData = await extractTextAdData(page, AD_TYPES_CONFIG.text_ad);
                if (textAdData.appName !== 'NOT_FOUND') result.appName = cleanName(textAdData.appName);
                if (textAdData.appSubtitle !== 'NOT_FOUND') result.appSubtitle = textAdData.appSubtitle;
                if (textAdData.storeLink !== 'NOT_FOUND') result.storeLink = textAdData.storeLink;
            } else {
                // Fallback: Try both extraction methods if detection failed
                console.log(`  🔄 Detection failed, trying both extraction methods as fallback...`);
                
                // Try image ad extraction first
                const imageAdData = await extractImageAdData(page, AD_TYPES_CONFIG.image_ad);
                if (imageAdData.imageUrl !== 'NOT_FOUND' || imageAdData.appName !== 'NOT_FOUND' || imageAdData.storeLink !== 'NOT_FOUND') {
                    console.log(`  ✅ Found image ad data via fallback`);
                    if (imageAdData.imageUrl !== 'NOT_FOUND') result.imageUrl = imageAdData.imageUrl;
                    if (imageAdData.appName !== 'NOT_FOUND') result.appName = cleanName(imageAdData.appName);
                    if (imageAdData.appSubtitle !== 'NOT_FOUND') result.appSubtitle = imageAdData.appSubtitle;
                    if (imageAdData.storeLink !== 'NOT_FOUND') result.storeLink = imageAdData.storeLink;
                } else {
                    // Try text ad extraction
                    const textAdData = await extractTextAdData(page, AD_TYPES_CONFIG.text_ad);
                    if (textAdData.appName !== 'NOT_FOUND' || textAdData.storeLink !== 'NOT_FOUND') {
                        console.log(`  ✅ Found text ad data via fallback`);
                        if (textAdData.appName !== 'NOT_FOUND') result.appName = cleanName(textAdData.appName);
                        if (textAdData.appSubtitle !== 'NOT_FOUND') result.appSubtitle = textAdData.appSubtitle;
                        if (textAdData.storeLink !== 'NOT_FOUND') result.storeLink = textAdData.storeLink;
                    } else {
                        console.log(`  ⚠️ No data found with either extraction method`);
                    }
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
            // FIXED: Always process exactly 3 items at a time (or remaining items if less than 3)
            const batchSize = Math.min(3, currentSessionSize - sessionProcessed);
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

                    // CRITICAL: Preserve rowIndex from original item
                    const originalRowIndex = item.rowIndex;
                    console.log(`  🚀 Row ${originalRowIndex + 1}: Starting ${item.url.substring(0, 40)}...`);
                    const data = await extractAllInOneVisit(
                        item.url,
                        browser,
                        item.needsMetadata,
                        item.needsVideoId,
                        item.existingStoreLink
                    );

                    // Ensure rowIndex is explicitly set from the original item
                    return {
                        url: item.url,
                        rowIndex: originalRowIndex, // Explicitly use original rowIndex
                        advertiserName: data.advertiserName,
                        storeLink: data.storeLink,
                        appName: data.appName,
                        videoId: data.videoId,
                        appSubtitle: data.appSubtitle,
                        imageUrl: data.imageUrl
                    };
                }));

                // Sort results by rowIndex to ensure correct order (extra safety)
                results.sort((a, b) => a.rowIndex - b.rowIndex);

                results.forEach(r => {
                    const linkStatus = r.storeLink && r.storeLink !== 'NOT_FOUND' && r.storeLink !== 'BLOCKED' ? '✅' : '❌';
                    console.log(`  ${linkStatus} Row ${r.rowIndex + 1}: Link=${r.storeLink?.substring(0, 45) || 'NONE'} | Name=${r.appName} | Writing to sheet row ${r.rowIndex + 1}`);
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