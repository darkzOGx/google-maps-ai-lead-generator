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
    fastMode = false, // Skip detail pages for 10x speed
}) => {
    const leads = [];
    const processedUrls = new Set();

    // Construct Google Maps search URL
    const searchQuery = `${category} in ${location}`;
    const searchUrl = `https://www.google.com/maps/search/${encodeURIComponent(searchQuery)}`;

    console.log(`🔍 Searching Google Maps: "${searchQuery}"`);

    // Set up proxy configuration
    const proxyConfiguration = proxyConfig?.useApifyProxy
        ? await Actor.createProxyConfiguration({
              groups: proxyConfig.apifyProxyGroups || ['RESIDENTIAL'],
              countryCode: proxyConfig.countryCode,
          })
        : undefined;

    const crawler = new PuppeteerCrawler({
        proxyConfiguration,
        maxConcurrency: Math.min(maxConcurrency, 3), // Lower concurrency to avoid rate limits
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 120,

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

                    // Wait for new content to load
                    await page.waitForTimeout(2000);

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

    // FAST MODE: Skip detail pages and return listing data only
    if (fastMode) {
        console.log(`⚡ Fast mode enabled - skipping detail page extraction`);
        console.log(`✅ Successfully scraped ${leads.length} businesses (fast mode)`);
        return leads;
    }

    console.log(`🔍 Now fetching details with browsers (lower concurrency)...`);

    // Now fetch detail pages with Puppeteer (JS needed for Google Maps)
    // Use LOWER concurrency (2-3 browsers) to prevent CPU overload
    const detailedLeads = [];

    const detailCrawler = new PuppeteerCrawler({
        proxyConfiguration,
        maxConcurrency: 1, // Further reduced to 1 for stability
        maxRequestRetries: 3, // Increased retries for network errors
        requestHandlerTimeoutSecs: 45, // Increased slightly for reliability
        navigationTimeoutSecs: 30, // Explicit navigation timeout

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
                // Wait for page to load (shorter timeout for speed)
                await page.waitForNetworkIdle({ timeout: 5000 }).catch(() => {});

                // Wait for main info panel to appear
                await page.waitForSelector('[role="main"]', { timeout: 8000 }).catch(() => {});

                // Extract phone number
                let phone = null;
                try {
                    const phoneButton = await page.$('button[data-item-id*="phone"]');
                    if (phoneButton) {
                        const ariaLabel = await phoneButton.evaluate((el) => el.getAttribute('aria-label'));
                        const match = ariaLabel?.match(/[\d\s\(\)\-\+]+/);
                        if (match) phone = match[0].trim();
                    }
                } catch (e) {}

                // Extract website
                let website = null;
                try {
                    const websiteLink = await page.$('a[data-item-id="authority"]');
                    if (websiteLink) {
                        website = await websiteLink.evaluate((el) => el.getAttribute('href'));
                    }
                } catch (e) {}

                // Extract address
                let address = null;
                try {
                    const addressButton = await page.$('button[data-item-id*="address"]');
                    if (addressButton) {
                        const ariaLabel = await addressButton.evaluate((el) => el.getAttribute('aria-label'));
                        address = ariaLabel?.replace('Address: ', '').trim() || null;
                    }
                } catch (e) {}

                // Extract category
                let category = null;
                try {
                    const categoryButton = await page.$('button[jsaction*="category"]');
                    if (categoryButton) {
                        category = await categoryButton.evaluate((el) => el.textContent?.trim());
                    }
                } catch (e) {}

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
                } else {
                    console.log(`🚫 Filtered: ${lead.businessName} - Reasons: ${filterReasons.join(', ')}`);
                }

            } catch (error) {
                console.warn(`⚠️ Failed to extract details for ${leadData.businessName}: ${error.message}`);
            }
        },

        failedRequestHandler({ request, error }) {
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
        },
    });

    // Enqueue detail page URLs for detail crawler
    const detailRequests = leads.map((lead) => ({
        url: lead.googleMapsUrl,
        userData: {
            businessName: lead.businessName,
            rating: lead.rating,
            reviewCount: lead.reviewCount,
        },
    }));

    await detailCrawler.run(detailRequests);

    console.log(`✅ Successfully scraped ${detailedLeads.length} businesses with full details`);

    return detailedLeads;
};
