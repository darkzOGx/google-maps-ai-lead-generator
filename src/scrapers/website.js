import { Actor } from 'apify';
import { CheerioCrawler } from 'crawlee';

/**
 * Extract email addresses from a website
 * @param {string} websiteUrl - URL of the website to scrape
 * @returns {Promise<string|null>} Email address or null if not found
 */
export const extractEmailFromWebsite = async (websiteUrl) => {
    if (!websiteUrl) return null;

    let foundEmail = null;
    const visitedUrls = new Set();
    const maxPagesToVisit = 5; // Check homepage + up to 4 other pages

    // Common patterns for email addresses
    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;

    // Blacklist of domains to ignore (false positives)
    const blacklistedDomains = [
        'example.com',
        'domain.com',
        'yourdomain.com',
        'yoursite.com',
        'email.com',
        'test.com',
        'sample.com',
        'wix.com',
        'wordpress.com',
        'squarespace.com',
        'weebly.com',
        'sentry.io',
        'gravatar.com',
        'w3.org',
        'placeholder.com',
        'yourcompany.com',
        'companyname.com',
        'schema.org',
        'javascript:',
        'mailto:',
        '.png',
        '.jpg',
        '.gif',
        '.svg',
    ];

    const crawler = new CheerioCrawler({
        maxRequestsPerCrawl: 1000, // High limit to avoid global counter issues
        maxConcurrency: 1,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 30,
        // Each email extraction gets its own queue
        requestQueue: await Actor.openRequestQueue(),

        async requestHandler({ $, request, enqueueLinks }) {
            // Skip if we already found an email
            if (foundEmail) return;

            visitedUrls.add(request.url);

            try {
                // Extract all text content from the page
                const pageText = $('body').text();

                // Find all email addresses
                const rawEmails = pageText.match(emailRegex);

                if (rawEmails && rawEmails.length > 0) {
                    // Clean and validate emails
                    const cleanedEmails = rawEmails
                        .map((email) => {
                            // Remove any non-email characters at start/end
                            // Match ONLY the valid email part
                            const cleanMatch = email.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
                            return cleanMatch ? cleanMatch[0] : null;
                        })
                        .filter((email) => {
                            if (!email) return false;

                            // Validate email format
                            const parts = email.split('@');
                            if (parts.length !== 2) return false;

                            const [localPart, domain] = parts;

                            // Basic validation
                            if (!localPart || !domain) return false;
                            if (localPart.length > 64 || domain.length > 255) return false;

                            // Filter out blacklisted domains
                            const domainLower = domain.toLowerCase();
                            return !blacklistedDomains.some((blacklisted) => domainLower.includes(blacklisted));
                        });

                    if (cleanedEmails.length > 0) {
                        // Prioritize certain email prefixes (more likely to be contact emails)
                        const priorityPrefixes = [
                            'info@',
                            'contact@',
                            'hello@',
                            'sales@',
                            'support@',
                            'admin@',
                            'office@',
                        ];

                        const priorityEmail = cleanedEmails.find((email) =>
                            priorityPrefixes.some((prefix) => email.toLowerCase().startsWith(prefix))
                        );

                        foundEmail = priorityEmail || cleanedEmails[0];

                        console.log(`📧 Found email: ${foundEmail} on ${request.url}`);
                        return; // Stop crawling
                    }
                }

                // If no email found on homepage, check contact/about pages
                if (visitedUrls.size === 1) {
                    // This is the first page (homepage), enqueue contact pages
                    const contactLinks = $('a[href*="contact"], a[href*="about"], a[href*="team"]')
                        .map((_, el) => $(el).attr('href'))
                        .get()
                        .filter((href) => href && !href.startsWith('#') && !href.startsWith('mailto:'))
                        .slice(0, 4); // Limit to 4 additional pages

                    for (const link of contactLinks) {
                        try {
                            const absoluteUrl = new URL(link, websiteUrl).href;
                            if (!visitedUrls.has(absoluteUrl)) {
                                await enqueueLinks({
                                    urls: [absoluteUrl],
                                    strategy: 'same-domain',
                                });
                            }
                        } catch (urlError) {
                            // Skip invalid URLs
                        }
                    }
                }
            } catch (error) {
                console.log(`⚠️ Error extracting email from ${request.url}: ${error.message}`);
            }
        },

        failedRequestHandler({ request, error }) {
            console.log(`⚠️ Failed to access ${request.url}: ${error.message}`);
        },
    });

    try {
        await crawler.run([websiteUrl]);
    } catch (error) {
        console.log(`⚠️ Email extraction failed for ${websiteUrl}: ${error.message}`);
        return null;
    }

    return foundEmail;
};

/**
 * Extract multiple contact details from a website
 * @param {string} websiteUrl - URL of the website
 * @returns {Promise<Object>} Object with email, phone, social links
 */
export const extractContactDetails = async (websiteUrl) => {
    const details = {
        email: null,
        phone: null,
        linkedin: null,
        facebook: null,
        twitter: null,
    };

    const crawler = new CheerioCrawler({
        maxRequestsPerCrawl: 3,
        maxConcurrency: 1,

        async requestHandler({ $, request }) {
            try {
                const pageText = $('body').text();

                // Extract email
                if (!details.email) {
                    const emailMatch = pageText.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
                    details.email = emailMatch ? emailMatch[0] : null;
                }

                // Extract phone
                if (!details.phone) {
                    const phoneMatch = pageText.match(/[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}/);
                    details.phone = phoneMatch ? phoneMatch[0] : null;
                }

                // Extract social links
                if (!details.linkedin) {
                    details.linkedin = $('a[href*="linkedin.com"]').attr('href') || null;
                }
                if (!details.facebook) {
                    details.facebook = $('a[href*="facebook.com"]').attr('href') || null;
                }
                if (!details.twitter) {
                    details.twitter = $('a[href*="twitter.com"]').attr('href') || null;
                }
            } catch (error) {
                // Fail silently
            }
        },
    });

    try {
        await crawler.run([websiteUrl]);
    } catch (error) {
        // Fail silently
    }

    return details;
};
