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
        maxConcurrency,
        maxRequestRetries: 3,
        requestHandlerTimeoutSecs: 120,

        launchContext: {
            launchOptions: {
                headless: true,
                args: ['--no-sandbox', '--disable-setuid-sandbox'],
            },
        },

        async requestHandler({ page, request }) {
            console.log(`🌐 Loading: ${request.url}`);

            try {
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
                        const debug = { selectors: {}, errors: [] };

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

                                // Extract rating (try multiple methods)
                                let rating = null;
                                const ratingEl = container.querySelector('[role="img"][aria-label*="star"]') ||
                                               container.querySelector('span[aria-label*="stars"]');
                                if (ratingEl) {
                                    const ratingText = ratingEl.getAttribute('aria-label') || '';
                                    const match = ratingText.match(/(\d+\.?\d*)\s*star/i);
                                    if (match) rating = parseFloat(match[1]);
                                }

                                // Extract review count
                                let reviewCount = 0;
                                const reviewEl = container.querySelector('span[aria-label*="review"]');
                                if (reviewEl) {
                                    const reviewText = reviewEl.getAttribute('aria-label') || '';
                                    const match = reviewText.match(/(\d+)/);
                                    if (match) reviewCount = parseInt(match[1]);
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

                    // Add new unique businesses
                    let addedCount = 0;
                    for (const card of newBusinessCards) {
                        if (!processedUrls.has(card.googleMapsUrl) && leads.length < maxResults) {
                            // Apply initial filters
                            if (filters.minRating && card.rating < filters.minRating) continue;
                            if (filters.minReviews && card.reviewCount < filters.minReviews) continue;

                            processedUrls.add(card.googleMapsUrl);
                            leads.push(card);
                            addedCount++;
                        }
                    }

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

                console.log(`✅ Collected ${leads.length} business cards, now fetching details...`);

                // Now visit each business detail page to get full information
                for (const [index, lead] of leads.entries()) {
                    try {
                        // Navigate to business detail page
                        await page.goto(lead.googleMapsUrl, { waitUntil: 'networkidle2', timeout: 30000 });
                        await page.waitForTimeout(1500);

                        // Extract detailed information
                        const details = await page.evaluate(() => {
                            const getElementText = (selector) => {
                                const el = document.querySelector(selector);
                                return el?.textContent?.trim() || null;
                            };

                            const getButtonAriaLabel = (text) => {
                                const button = Array.from(document.querySelectorAll('button')).find((btn) =>
                                    btn.getAttribute('aria-label')?.includes(text)
                                );
                                return button?.getAttribute('aria-label') || null;
                            };

                            // Extract phone number
                            const phoneLabel = getButtonAriaLabel('Phone:');
                            const phone = phoneLabel?.match(/[\d\s\(\)\-\+]+/)?.[0]?.trim() || null;

                            // Extract website
                            const websiteLink = document.querySelector('a[data-item-id="authority"]');
                            const website = websiteLink?.href || null;

                            // Extract address
                            const addressButton = Array.from(document.querySelectorAll('button')).find((btn) =>
                                btn.getAttribute('data-item-id')?.includes('address')
                            );
                            const address = addressButton?.getAttribute('aria-label')?.replace('Address: ', '') || null;

                            // Extract category
                            const categoryButton = document.querySelector('button[jsaction*="category"]');
                            const category = categoryButton?.textContent?.trim() || null;

                            // Check if listing is claimed
                            const claimed = document.querySelector('[aria-label*="Claimed"]') !== null;

                            // Extract description
                            const description = getElementText('[class*="description"]');

                            // Extract hours (if available)
                            const hoursButton = Array.from(document.querySelectorAll('button')).find((btn) =>
                                btn.getAttribute('aria-label')?.includes('Hours')
                            );
                            const hours = hoursButton?.getAttribute('aria-label') || null;

                            // Check for social media links
                            const socialLinks = {
                                linkedin: document.querySelector('a[href*="linkedin.com"]')?.href || null,
                                facebook: document.querySelector('a[href*="facebook.com"]')?.href || null,
                                twitter: document.querySelector('a[href*="twitter.com"]')?.href || null,
                                instagram: document.querySelector('a[href*="instagram.com"]')?.href || null,
                            };

                            return {
                                phone,
                                website,
                                address,
                                category,
                                claimed,
                                description,
                                hours,
                                socialLinks,
                            };
                        });

                        // Merge details into lead
                        Object.assign(lead, details);

                        // Apply additional filters
                        let shouldInclude = true;

                        if (filters.hasWebsite && !lead.website) {
                            shouldInclude = false;
                        }

                        if (filters.claimedListing && !lead.claimed) {
                            shouldInclude = false;
                        }

                        if (filters.hasSocialMedia) {
                            const hasSocial = Object.values(lead.socialLinks).some((link) => link !== null);
                            if (!hasSocial) shouldInclude = false;
                        }

                        // Mark for removal if doesn't meet criteria
                        if (!shouldInclude) {
                            lead._remove = true;
                        }

                        // Progress logging
                        if ((index + 1) % 5 === 0) {
                            console.log(`⏳ Fetched details for ${index + 1}/${leads.length} businesses`);
                        }

                    } catch (detailError) {
                        console.warn(`⚠️ Failed to get details for ${lead.businessName}`, {
                            error: detailError.message,
                        });
                        // Keep the lead with basic info
                    }
                }

            } catch (error) {
                console.error('❌ Error during scraping', {
                    error: error.message,
                    url: request.url,
                });
                throw error;
            }
        },

        failedRequestHandler({ request, error }) {
            console.error(`❌ Request failed: ${request.url}`, {
                error: error.message,
            });
        },
    });

    // Run the crawler
    await crawler.run([searchUrl]);

    // Filter out leads marked for removal
    const filteredLeads = leads.filter((lead) => !lead._remove);

    console.log(`✅ Successfully scraped ${filteredLeads.length} businesses from Google Maps`);

    return filteredLeads;
};
