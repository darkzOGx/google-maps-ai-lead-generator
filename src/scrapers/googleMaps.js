import { Actor } from 'apify';
import { PuppeteerCrawler } from 'crawlee';

/**
 * Scrape Google Maps for business listings
 * @param {Object} params - Scraping parameters
 * @param {string} params.category - Business category to search (e.g., "software companies")
 * @param {string} params.location - Location to search (e.g., "San Francisco, CA")
 * @param {number} params.maxResults - Maximum number of results to return
 * @param {Object} params.filters - Quality filters (minRating, minReviews, etc.)
 * @param {Object} params.proxyConfig - Proxy configuration
 * @param {number} params.maxConcurrency - Max concurrent requests
 * @returns {Promise<Array>} Array of lead objects
 */
export const scrapeGoogleMaps = async ({
    category,
    location,
    maxResults = 100,
    filters = {},
    proxyConfig,
    maxConcurrency = 5,
    detailConcurrency = null, // Separate concurrency for detail page crawling (null = use default based on proxies)
    fastMode = false, // Skip detail pages for 10x speed
    language = 'en', // Language code
    skipClosedPlaces = true, // Filter out permanently closed places
    enrichment = {}, // Enrichment options (extractReviews, maxReviewsPerPlace, etc.)
    onLeadScraped = null, // Callback function called for each successfully scraped lead
}) => {
    const leads = [];
    const processedUrls = new Set();

    // Construct Google Maps search URL with language
    const searchQuery = `${category} in ${location}`;
    const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(searchQuery)}?hl=${language}`;

    console.log(`🔍 Searching Google Maps: "${searchQuery}" (language: ${language})`);

    // Set up proxy configuration with fallback
    let proxyConfiguration;
    if (proxyConfig?.useApifyProxy) {
        try {
            // Use user's preferred proxy groups, or auto-select if not specified
            const proxyGroups = (proxyConfig.apifyProxyGroups && proxyConfig.apifyProxyGroups.length > 0)
                ? proxyConfig.apifyProxyGroups
                : undefined; // Let Apify auto-select from available proxies

            proxyConfiguration = await Actor.createProxyConfiguration({
                groups: proxyGroups,
                countryCode: proxyConfig.countryCode,
            });
            console.log(`🔒 Using ${proxyGroups ? 'proxy groups: ' + proxyGroups.join(', ') : 'auto-selected proxies'}`);
        } catch (proxyError) {
            console.warn(`⚠️ Proxy setup failed: ${proxyError.message}`);
            // Fallback: Try without country restriction (auto-select proxies)
            try {
                proxyConfiguration = await Actor.createProxyConfiguration();
                console.log('🔒 Using fallback proxies (auto-selected)');
            } catch (fallbackError) {
                console.warn('⚠️ All proxies failed, continuing without proxies');
                proxyConfiguration = undefined;
            }
        }
    }

    const crawler = new PuppeteerCrawler({
        proxyConfiguration,
        // Respect user's performance preset choice (Balanced/Fast/Turbo)
        maxConcurrency: maxConcurrency,
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 90, // Reduced from 120 for faster failures
        navigationTimeoutSecs: 45, // Fail faster on navigation issues

        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                ],
            },
            useChrome: true, // Use full Chrome instead of Chromium
        },

        // Set realistic browser context
        preNavigationHooks: [
            async ({ page, request }) => {
                // Set realistic viewport
                await page.setViewport({ width: 1920, height: 1080 });

                // Set realistic user agent
                await page.setUserAgent(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                );

                // Remove webdriver flag
                await page.evaluateOnNewDocument(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                });
            },
        ],

        async requestHandler({ page, request }) {
            console.log(`🌐 Loading: ${request.url}`);

            // Handle different request types
            if (!request.label || request.label === 'SEARCH') {
                // This is the main search page
                try {
                    // Wait for page to fully load
                    await page.waitForNetworkIdle({ timeout: 15000 }).catch(() => {
                        console.log('⏳ Network not idle after 15s, continuing anyway...');
                    });

                    // Wait for results to load
                    await page.waitForSelector('[role="feed"]', { timeout: 30000 });
                    console.log('✅ Google Maps results loaded');

                // Scroll to load more results (bypass 120-result limit)
                let scrollAttempts = 0;
                const maxScrollAttempts = Math.ceil(maxResults / 20); // ~20 results per scroll

                while (leads.length < maxResults && scrollAttempts < maxScrollAttempts) {
                    // Scroll the results panel
                    await page.evaluate(() => {
                        const feed = document.querySelector('[role="feed"]');
                        if (feed) {
                            feed.scrollTop = feed.scrollHeight;
                        }
                    });

                    // Wait for new content to load (faster in fast mode)
                    await page.waitForTimeout(fastMode ? 1000 : 2000);

                    // Extract visible business cards with updated selectors
                    const extractionResult = await page.evaluate(() => {
                        const cards = [];
                        const debug = { selectors: {}, errors: [], reviewExtractionStats: {} };

                        // Find all business listing containers in the feed
                        const feed = document.querySelector('[role="feed"]');
                        if (!feed) {
                            debug.feedFound = false;
                            return { cards, debug };
                        }
                        debug.feedFound = true;

                        // Try multiple selector strategies for business cards
                        debug.selectors.articles = feed.querySelectorAll('div[role="article"]').length;
                        debug.selectors.nv2pk = feed.querySelectorAll('div.Nv2PK').length;
                        debug.selectors.placeLinks = feed.querySelectorAll('a[href*="/maps/place/"]').length;

                        const listItems = feed.querySelectorAll('div[role="article"], div.Nv2PK, a[href*="/maps/place/"]');
                        debug.totalElements = listItems.length;

                        const processedUrls = new Set();

                        listItems.forEach((item) => {
                            try {
                                // Find the place link
                                let link = item.querySelector('a[href*="/maps/place/"]') ||
                                          (item.tagName === 'A' && item.href?.includes('/maps/place/') ? item : null);

                                if (!link) return;

                                const href = link.href;
                                if (!href || processedUrls.has(href)) return;
                                processedUrls.add(href);

                                // Find the parent container (article or closest div)
                                const container = item.closest('[role="article"]') ||
                                                 item.closest('div.Nv2PK') ||
                                                 link.closest('div[jsaction]') ||
                                                 item;

                                if (!container) return;

                                // Extract business name (try multiple selectors)
                                let name = null;
                                const nameSelectors = [
                                    '[class*="fontHeadline"]',
                                    '[class*="fontBodyMedium"]',
                                    'div[role="heading"]',
                                    '.fontDisplayLarge',
                                    'a[href*="/maps/place/"] div'
                                ];

                                for (const selector of nameSelectors) {
                                    const nameEl = container.querySelector(selector);
                                    if (nameEl?.textContent?.trim()) {
                                        name = nameEl.textContent.trim();
                                        break;
                                    }
                                }

                                // If still no name, try the link's aria-label
                                if (!name && link.getAttribute('aria-label')) {
                                    name = link.getAttribute('aria-label');
                                }

                                // Extract rating with multiple fallback strategies
                                let rating = null;
                                let ratingStrategy = null;

                                // Strategy 1: aria-label with "star"
                                let ratingEl = container.querySelector('[role="img"][aria-label*="star"]') ||
                                              container.querySelector('span[aria-label*="stars"]') ||
                                              container.querySelector('[aria-label*="star"]');

                                if (ratingEl) {
                                    const ratingText = ratingEl.getAttribute('aria-label') || '';
                                    const match = ratingText.match(/(\d+\.?\d*)\s*star/i);
                                    if (match) {
                                        rating = parseFloat(match[1]);
                                        ratingStrategy = 'aria-label-star';
                                    }
                                }

                                // Strategy 2: Look for decimal number pattern (e.g., "4.7" or "3.3")
                                if (rating === null) {
                                    const allText = container.textContent || '';
                                    // Match rating-like numbers (1.0-5.0 range)
                                    const ratingMatch = allText.match(/\b([1-5]\.\d)\b/);
                                    if (ratingMatch) {
                                        rating = parseFloat(ratingMatch[1]);
                                        ratingStrategy = 'text-decimal';
                                    }
                                }

                                // Strategy 3: Look in spans for rating numbers
                                if (rating === null) {
                                    const spans = container.querySelectorAll('span');
                                    for (const span of spans) {
                                        const text = span.textContent?.trim() || '';
                                        // Match X.X format where X is 1-5
                                        if (/^[1-5]\.\d$/.test(text)) {
                                            rating = parseFloat(text);
                                            ratingStrategy = 'span-rating';
                                            break;
                                        }
                                    }
                                }

                                // Track which strategies work
                                if (ratingStrategy) {
                                    debug.ratingExtractionStats = debug.ratingExtractionStats || {};
                                    debug.ratingExtractionStats[ratingStrategy] = (debug.ratingExtractionStats[ratingStrategy] || 0) + 1;
                                } else {
                                    debug.ratingExtractionStats = debug.ratingExtractionStats || {};
                                    debug.ratingExtractionStats['failed'] = (debug.ratingExtractionStats['failed'] || 0) + 1;
                                }

                                // Extract review count with multiple fallback strategies
                                let reviewCount = 0;
                                let reviewStrategy = null;

                                // Strategy 1: Look for aria-label with "review"
                                let reviewEl = container.querySelector('span[aria-label*="review"]') ||
                                              container.querySelector('button[aria-label*="review"]');

                                if (reviewEl) {
                                    const reviewText = reviewEl.getAttribute('aria-label') || '';
                                    const match = reviewText.match(/(\d+)/);
                                    if (match) {
                                        reviewCount = parseInt(match[1]);
                                        reviewStrategy = 'aria-label';
                                    }
                                }

                                // Strategy 2: Look for text content with parentheses (e.g., "(123)")
                                if (reviewCount === 0) {
                                    const allText = container.textContent || '';
                                    const parenMatch = allText.match(/\((\d+)\)/);
                                    if (parenMatch) {
                                        reviewCount = parseInt(parenMatch[1]);
                                        reviewStrategy = 'parentheses';
                                    }
                                }

                                // Strategy 3: Look for rating element siblings
                                if (reviewCount === 0 && ratingEl) {
                                    const parent = ratingEl.parentElement;
                                    if (parent) {
                                        const siblingText = parent.textContent || '';
                                        const numMatch = siblingText.match(/(\d+)\s*reviews?/i);
                                        if (numMatch) {
                                            reviewCount = parseInt(numMatch[1]);
                                            reviewStrategy = 'rating-sibling';
                                        }
                                    }
                                }

                                // Strategy 4: Look for span with review numbers next to rating
                                if (reviewCount === 0) {
                                    const spans = container.querySelectorAll('span');
                                    for (const span of spans) {
                                        const text = span.textContent?.trim() || '';
                                        // Match patterns like "123 reviews" or just "(123)"
                                        if (/^\(?\d+\)?$/.test(text) && text.length <= 6) {
                                            const num = parseInt(text.replace(/[()]/g, ''));
                                            if (num > 0 && num < 1000000) { // Sanity check
                                                reviewCount = num;
                                                reviewStrategy = 'span-number';
                                                break;
                                            }
                                        }
                                    }
                                }

                                // Track which strategies work
                                if (reviewStrategy) {
                                    debug.reviewExtractionStats[reviewStrategy] = (debug.reviewExtractionStats[reviewStrategy] || 0) + 1;
                                } else {
                                    debug.reviewExtractionStats['failed'] = (debug.reviewExtractionStats['failed'] || 0) + 1;
                                }

                                if (name && href) {
                                    cards.push({
                                        businessName: name,
                                        googleMapsUrl: href,
                                        rating: rating,
                                        reviewCount: reviewCount,
                                    });
                                }
                            } catch (err) {
                                debug.errors.push(err.message);
                            }
                        });

                        debug.cardsExtracted = cards.length;
                        return { cards, debug };
                    });

                    const newBusinessCards = extractionResult.cards;

                    // Log diagnostic info
                    console.log(`🔍 DIAGNOSTICS:`, JSON.stringify(extractionResult.debug, null, 2));
                    console.log(`📊 Found ${extractionResult.debug.totalElements} elements, extracted ${extractionResult.debug.cardsExtracted} cards`);

                    // Log sample cards for debugging
                    if (newBusinessCards.length > 0) {
                        console.log(`📋 Sample card 1:`, JSON.stringify(newBusinessCards[0], null, 2));
                        if (newBusinessCards.length > 1) {
                            console.log(`📋 Sample card 2:`, JSON.stringify(newBusinessCards[1], null, 2));
                        }
                    }

                    // Add new unique businesses
                    let addedCount = 0;
                    let filteredOut = { noRating: 0, noReviews: 0, duplicate: 0 };

                    for (const card of newBusinessCards) {
                        if (processedUrls.has(card.googleMapsUrl)) {
                            filteredOut.duplicate++;
                            continue;
                        }

                        if (leads.length >= maxResults) break;

                        // Apply initial filters with logging
                        // Allow null ratings to pass (will get rating from detail page)
                        if (filters.minRating && card.rating !== null && card.rating < filters.minRating) {
                            filteredOut.noRating++;
                            continue;
                        }
                        if (filters.minReviews && card.reviewCount < filters.minReviews) {
                            filteredOut.noReviews++;
                            continue;
                        }

                        processedUrls.add(card.googleMapsUrl);
                        leads.push(card);
                        addedCount++;
                    }

                    console.log(`🚫 Filtered out: ${JSON.stringify(filteredOut)}`);

                    scrollAttempts++;
                    console.log(
                        `📊 Scroll ${scrollAttempts}: Found ${addedCount} new businesses (total: ${leads.length}/${maxResults})`
                    );

                    // Stop if we haven't found new results in this scroll
                    if (addedCount === 0) {
                        console.log('⚠️ No new results found, stopping scroll');
                        break;
                    }
                }

                console.log(`✅ Collected ${leads.length} business cards`);

                } catch (error) {
                    console.error('❌ Error during search scraping', {
                        error: error.message,
                        url: request.url,
                    });

                    // Diagnostic: Check what's on the page
                    try {
                        const pageTitle = await page.title();
                        const pageUrl = page.url();
                        console.log(`🔍 Page diagnostics: title="${pageTitle}", url="${pageUrl}"`);

                        // Check for common Google block messages
                        const bodyText = await page.evaluate(() => document.body.innerText);
                        if (bodyText.includes('unusual traffic') || bodyText.includes('CAPTCHA') || bodyText.includes('detected automated queries')) {
                            console.error('🚫 DETECTED: Google is showing a bot detection page!');
                        }
                    } catch (diagError) {
                        console.log('⚠️ Could not get page diagnostics');
                    }

                    throw error;
                }
            }
        },

        failedRequestHandler({ request, error }) {
            console.error(`❌ Request failed: ${request.url}`, {
                error: error.message,
            });
        },
    });

    // Run the Puppeteer crawler to get listing cards
    await crawler.run([searchUrl]);

    console.log(`📋 Collected ${leads.length} business cards`);

    // FAST MODE: Skip detail pages but still save listing data
    if (fastMode) {
        console.log(`⚡ Fast mode enabled - skipping detail page extraction`);

        // Call callback for each lead to save data incrementally
        if (onLeadScraped) {
            for (const lead of leads) {
                try {
                    await onLeadScraped(lead);
                    console.log(`💾 Saved (fast mode): ${lead.businessName}`);
                } catch (callbackError) {
                    console.error(`❌ Fast mode save failed for ${lead.businessName}: ${callbackError.message}`);
                }
            }
        }

        console.log(`✅ Successfully scraped ${leads.length} businesses (fast mode)`);
        return leads;
    }

    console.log(`🔍 Now fetching details with browsers (lower concurrency)...`);

    // Now fetch detail pages with Puppeteer (JS needed for Google Maps)
    // Use LOWER concurrency (2-3 browsers) to prevent CPU overload
    const detailedLeads = [];

    // Adjust settings based on proxy usage and performance preset
    const usingProxies = proxyConfiguration !== undefined;

    // Determine detail concurrency: use custom value if provided, otherwise use defaults
    let detailConcurrencyValue;
    if (detailConcurrency !== null) {
        detailConcurrencyValue = detailConcurrency; // User-specified concurrency (from performance preset)
    } else {
        detailConcurrencyValue = usingProxies ? 3 : 1; // Default: 3 with proxies, 1 without
    }

    const detailSettings = {
        concurrency: detailConcurrencyValue,
        navTimeout: 60,
        handlerTimeout: 90,
        retries: usingProxies ? 2 : 3
    };

    console.log(`⚙️ Detail crawler settings: ${usingProxies ? 'WITH' : 'WITHOUT'} proxies (concurrency: ${detailSettings.concurrency}, timeout: ${detailSettings.navTimeout}s)`);

    const detailCrawler = new PuppeteerCrawler({
        proxyConfiguration,
        maxConcurrency: detailSettings.concurrency,
        maxRequestRetries: detailSettings.retries,
        requestHandlerTimeoutSecs: detailSettings.handlerTimeout,
        navigationTimeoutSecs: detailSettings.navTimeout,

        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                ],
            },
            useChrome: true,
        },

        // Same anti-bot measures
        preNavigationHooks: [
            async ({ page }) => {
                await page.setViewport({ width: 1920, height: 1080 });
                await page.setUserAgent(
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                );
                await page.evaluateOnNewDocument(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                });
            },
        ],

        async requestHandler({ page, request }) {
            const leadData = request.userData;
            console.log(`🔍 Fetching details: ${leadData.businessName}`);

            try {
                // Wait for page to load (increased timeouts to handle slow pages)
                const networkIdleTimeout = usingProxies ? 10000 : 10000;
                const selectorTimeout = usingProxies ? 15000 : 15000;

                await page.waitForNetworkIdle({ timeout: networkIdleTimeout }).catch(() => {});

                // Wait for main info panel to appear
                await page.waitForSelector('[role="main"]', { timeout: selectorTimeout }).catch(() => {});

                // Extract phone number with multiple strategies
                let phone = null;
                try {
                    phone = await page.evaluate(() => {
                        // Strategy 1: Look for phone in buttons/links with aria-label
                        const buttons = Array.from(document.querySelectorAll('button, a, div'));
                        for (const el of buttons) {
                            const ariaLabel = el.getAttribute('aria-label') || '';
                            const text = el.textContent || '';

                            // Check aria-label for phone
                            if (ariaLabel.toLowerCase().includes('phone')) {
                                const match = ariaLabel.match(/[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}/);
                                if (match) return match[0];
                            }

                            // Check text content for phone
                            const phoneMatch = text.match(/[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4}/);
                            if (phoneMatch && !text.includes('@')) { // exclude emails
                                return phoneMatch[0];
                            }
                        }

                        // Strategy 2: Search entire page for phone pattern
                        const bodyText = document.body.textContent || '';
                        const patterns = [
                            /\(\d{3}\)\s?\d{3}-\d{4}/,  // (123) 456-7890
                            /\d{3}-\d{3}-\d{4}/,        // 123-456-7890
                            /\+\d{1,2}\s?\(\d{3}\)\s?\d{3}-\d{4}/  // +1 (123) 456-7890
                        ];

                        for (const pattern of patterns) {
                            const match = bodyText.match(pattern);
                            if (match) return match[0];
                        }

                        return null;
                    });

                    if (phone) {
                        console.log(`📞 Found phone: ${phone}`);
                    }
                } catch (e) {
                    console.warn(`⚠️ Phone extraction error: ${e.message}`);
                }

                // Extract website with multiple fallback strategies
                let website = null;
                try {
                    website = await page.evaluate(() => {
                        // Strategy 1: Look for links in action buttons
                        const buttons = Array.from(document.querySelectorAll('button, a'));
                        for (const btn of buttons) {
                            const ariaLabel = btn.getAttribute('aria-label') || '';
                            if (ariaLabel.toLowerCase().includes('website')) {
                                // Extract URL from onclick or href
                                const href = btn.getAttribute('href') || btn.onclick?.toString() || '';
                                const match = href.match(/https?:\/\/[^\s"']+/);
                                if (match) return match[0];
                            }
                        }

                        // Strategy 2: Look for any external link (not google/social)
                        const links = Array.from(document.querySelectorAll('a[href]'));
                        for (const link of links) {
                            const href = link.href || '';
                            const text = (link.textContent || '').toLowerCase();

                            // Skip social media and google links
                            if (href.includes('google.com') || href.includes('facebook.com') ||
                                href.includes('instagram.com') || href.includes('twitter.com') ||
                                href.includes('linkedin.com') || href.includes('youtube.com')) {
                                continue;
                            }

                            // Look for http links or text containing "website"
                            if (href.startsWith('http') || text.includes('website')) {
                                return href;
                            }
                        }

                        return null;
                    });

                    if (website) {
                        console.log(`🌐 Found website: ${website}`);
                    }
                } catch (e) {
                    console.warn(`⚠️ Website extraction error: ${e.message}`);
                }

                // Extract address with multiple strategies
                let address = null;
                try {
                    address = await page.evaluate(() => {
                        // Strategy 1: Look for address in buttons with aria-label
                        const buttons = Array.from(document.querySelectorAll('button, a, div'));
                        for (const el of buttons) {
                            const ariaLabel = el.getAttribute('aria-label') || '';
                            if (ariaLabel.toLowerCase().includes('address')) {
                                // Extract address from aria-label
                                const addr = ariaLabel.replace(/address:\s*/i, '').trim();
                                if (addr.length > 10) return addr;
                            }
                        }

                        // Strategy 2: Look for address patterns in page text
                        const bodyText = document.body.textContent || '';

                        // Pattern 1: Street number + street name + city + state + ZIP
                        const fullPattern = /\d+\s+[A-Za-z0-9\s\.#]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}(-\d{4})?/;
                        let match = bodyText.match(fullPattern);
                        if (match) return match[0];

                        // Pattern 2: Simplified pattern (street, city, state)
                        const simplePattern = /\d+\s+[A-Za-z0-9\s\.#]+[,\s]+[A-Za-z\s]+[,\s]+[A-Z]{2}/;
                        match = bodyText.match(simplePattern);
                        if (match) return match[0];

                        return null;
                    });

                    if (address) {
                        console.log(`📍 Found address: ${address}`);
                    }
                } catch (e) {
                    console.warn(`⚠️ Address extraction error: ${e.message}`);
                }

                // Extract category
                let category = null;
                try {
                    const categoryButton = await page.$('button[jsaction*="category"]');
                    if (categoryButton) {
                        category = await categoryButton.evaluate((el) => el.textContent?.trim());
                    }
                } catch (e) {}

                // Check if place is permanently closed
                const isClosed = (await page.$('[aria-label*="Permanently closed"]')) !== null ||
                                (await page.$('[aria-label*="Closed permanently"]')) !== null ||
                                (await page.evaluate(() => document.body.textContent.includes('Permanently closed')));

                // Skip if closed and filter is enabled
                if (skipClosedPlaces && isClosed) {
                    console.log(`⏭️ Skipped (closed): ${leadData.businessName}`);
                    return;
                }

                // Check if listing is claimed (business owner verified)
                // Most legitimate businesses ARE claimed but don't show explicit badge
                // Better heuristic: if no "Claim this business" button, it's already claimed
                let claimed = (await page.$('[aria-label*="Claim this business"]')) === null;

                // Fallback: if has website AND phone, assume claimed
                if (!claimed && website && phone) {
                    claimed = true; // Likely claimed if they added full contact info
                }

                // Extract social media links
                const socialLinks = {
                    linkedin: await page.$eval('a[href*="linkedin.com"]', (el) => el.href).catch(() => null),
                    facebook: await page.$eval('a[href*="facebook.com"]', (el) => el.href).catch(() => null),
                    twitter: await page.$eval('a[href*="twitter.com"]', (el) => el.href).catch(() => null),
                    instagram: await page.$eval('a[href*="instagram.com"]', (el) => el.href).catch(() => null),
                };

                // Extract reviews if enabled
                let reviews = [];
                if (leadData.extractReviews && leadData.maxReviewsPerPlace > 0) {
                    try {
                        console.log(`📝 Extracting ${leadData.maxReviewsPerPlace} reviews for: ${leadData.businessName}`);

                        // Click on reviews tab
                        const reviewsButton = await page.$('button[aria-label*="Reviews"]');
                        if (reviewsButton) {
                            await reviewsButton.click();
                            await page.waitForTimeout(2000); // Wait for reviews to load

                            // Scroll to load more reviews
                            const reviewsContainer = await page.$('[role="feed"]');
                            if (reviewsContainer) {
                                for (let i = 0; i < Math.ceil(leadData.maxReviewsPerPlace / 10); i++) {
                                    await page.evaluate(() => {
                                        const feed = document.querySelector('[role="feed"]');
                                        if (feed) feed.scrollTop = feed.scrollHeight;
                                    });
                                    await page.waitForTimeout(1000);
                                }

                                // Extract review data
                                reviews = await page.evaluate((maxReviews) => {
                                    const reviewElements = document.querySelectorAll('[data-review-id]');
                                    const extractedReviews = [];

                                    for (let i = 0; i < Math.min(reviewElements.length, maxReviews); i++) {
                                        const reviewEl = reviewElements[i];
                                        try {
                                            const ratingEl = reviewEl.querySelector('[role="img"][aria-label*="star"]');
                                            const ratingText = ratingEl?.getAttribute('aria-label') || '';
                                            const ratingMatch = ratingText.match(/(\d+)\s*star/i);
                                            const rating = ratingMatch ? parseInt(ratingMatch[1]) : null;

                                            const textEl = reviewEl.querySelector('[class*="review-text"], [class*="MyEned"]');
                                            const text = textEl?.textContent?.trim() || '';

                                            const authorEl = reviewEl.querySelector('[class*="author"], [class*="d4r55"]');
                                            const author = authorEl?.textContent?.trim() || 'Anonymous';

                                            const dateEl = reviewEl.querySelector('[class*="date"], [class*="rsqaWe"]');
                                            const date = dateEl?.textContent?.trim() || '';

                                            if (text || rating) {
                                                extractedReviews.push({ rating, text, author, date });
                                            }
                                        } catch (e) {
                                            // Skip failed review extraction
                                        }
                                    }

                                    return extractedReviews;
                                }, leadData.maxReviewsPerPlace);

                                console.log(`✅ Extracted ${reviews.length} reviews`);
                            }
                        }
                    } catch (reviewError) {
                        console.warn(`⚠️ Failed to extract reviews: ${reviewError.message}`);
                    }
                }

                // Create complete lead object
                const lead = {
                    businessName: leadData.businessName,
                    googleMapsUrl: request.url,
                    rating: leadData.rating,
                    reviewCount: leadData.reviewCount,
                    phone,
                    website,
                    address,
                    category,
                    claimed,
                    socialLinks,
                    reviews, // Customer reviews (if extracted)
                };

                // Debug: Log extracted data
                console.log(`📊 Extracted data for ${lead.businessName}:`, {
                    website: website || 'NONE',
                    claimed: claimed ? 'YES' : 'NO',
                    phone: phone || 'NONE',
                    address: address || 'NONE',
                });

                // Apply additional filters
                let shouldInclude = true;
                const filterReasons = [];

                if (filters.hasWebsite && !lead.website) {
                    shouldInclude = false;
                    filterReasons.push('no website');
                }

                if (filters.claimedListing && !lead.claimed) {
                    shouldInclude = false;
                    filterReasons.push('not claimed');
                }

                if (filters.hasSocialMedia) {
                    const hasSocial = Object.values(lead.socialLinks).some((link) => link !== null);
                    if (!hasSocial) {
                        shouldInclude = false;
                        filterReasons.push('no social media');
                    }
                }

                if (shouldInclude) {
                    detailedLeads.push(lead);
                    console.log(`✅ Added: ${lead.businessName} (${lead.website || 'no website'})`);

                    // Call callback immediately to save data incrementally
                    if (onLeadScraped) {
                        try {
                            await onLeadScraped(lead);
                        } catch (callbackError) {
                            console.error(`❌ onLeadScraped callback failed: ${callbackError.message}`);
                        }
                    }
                } else {
                    console.log(`🚫 Filtered: ${lead.businessName} - Reasons: ${filterReasons.join(', ')}`);
                }

            } catch (error) {
                console.warn(`⚠️ Failed to extract details for ${leadData.businessName}: ${error.message}`);
            }
        },

        async failedRequestHandler({ request, error }) {
            console.warn(`⚠️ Request failed for ${request.userData.businessName}: ${error.message}`);

            // Still add partial data even if detail fetch fails
            const partialLead = {
                businessName: request.userData.businessName,
                googleMapsUrl: request.url,
                rating: request.userData.rating,
                reviewCount: request.userData.reviewCount,
                phone: null,
                website: null,
                address: null,
                category: null,
                claimed: false,
                socialLinks: {},
                error: `Failed to fetch details: ${error.message}`,
            };

            detailedLeads.push(partialLead);
            console.log(`⚠️ Added partial data for: ${partialLead.businessName}`);

            // Call callback immediately to save partial data
            if (onLeadScraped) {
                try {
                    await onLeadScraped(partialLead);
                } catch (callbackError) {
                    console.error(`❌ onLeadScraped callback failed: ${callbackError.message}`);
                }
            }
        },
    });

    // Enqueue detail page URLs for detail crawler
    const detailRequests = leads.map((lead) => ({
        url: lead.googleMapsUrl,
        userData: {
            businessName: lead.businessName,
            rating: lead.rating,
            reviewCount: lead.reviewCount,
            extractReviews: enrichment?.extractReviews || false,
            maxReviewsPerPlace: enrichment?.maxReviewsPerPlace || 10,
        },
    }));

    await detailCrawler.run(detailRequests);

    console.log(`✅ Successfully scraped ${detailedLeads.length} businesses with full details`);

    return detailedLeads;
};
