import { Actor } from 'apify';
import { scrapeGoogleMaps } from './scrapers/googleMaps.js';
import { extractEmailFromWebsite } from './scrapers/website.js';
import { calculateLeadScore } from './scoring/leadScore.js';
import { sendWebhook } from './integrations/webhook.js';

// Initialize the Apify actor
await Actor.init();

try {
    // Get input from Apify platform UI
    const rawInput = await Actor.getInput();

    console.log('📥 Input received:', JSON.stringify(rawInput, null, 2));

    // Determine scraping mode
    const scrapingMode = rawInput.scrapingMode || 'enriched';
    const isBasicMode = scrapingMode === 'basic';
    const isEnrichedMode = scrapingMode === 'enriched';

    // Determine performance preset (concurrency settings)
    // PRODUCTION FIX: Reduced defaults to prevent CPU overload
    const performancePreset = rawInput.performancePreset || 'balanced';
    const performanceConfig = {
        balanced: { maxConcurrency: 3, detailConcurrency: 2, memoryMB: 8192 },
        fast: { maxConcurrency: 5, detailConcurrency: 3, memoryMB: 16384 },
        turbo: { maxConcurrency: 8, detailConcurrency: 5, memoryMB: 32768 },
    };
    const perfSettings = performanceConfig[performancePreset] || performanceConfig.balanced;

    // Transform new flat structure to internal format
    const input = {
        searchQueries: [
            {
                category: rawInput.searchQuery || 'software companies',
                location: rawInput.location || 'San Francisco, CA',
                maxResults: rawInput.maxResults || 50,
            }
        ],
        language: rawInput.language || 'en',
        skipClosedPlaces: rawInput.skipClosedPlaces !== false,
        fastMode: isBasicMode, // Basic mode = fast (no detail pages), Enriched = slow (full scraping)
        filters: {
            minRating: rawInput.minRating || 0,
            minReviews: rawInput.minReviews || 0,
            hasWebsite: isEnrichedMode ? (rawInput.hasWebsite !== false) : false, // Only filter by website in enriched mode
            claimedListing: rawInput.claimedListing || false,
            hasSocialMedia: rawInput.hasSocialMedia || false,
        },
        enrichment: {
            extractEmails: isEnrichedMode && (rawInput.extractEmails !== false), // Only extract emails in enriched mode
            extractReviews: rawInput.extractReviews || false,
            maxReviewsPerPlace: rawInput.maxReviewsPerPlace || 10,
            validateContacts: false,
            companyData: false,
            techStack: false,
            findDecisionMakers: false,
        },
        scoring: {
            enableScoring: isEnrichedMode && (rawInput.enableScoring !== false), // Only score in enriched mode
            idealCustomerProfile: {
                industries: Array.isArray(rawInput.targetIndustries) && rawInput.targetIndustries.length > 0
                    ? Object.fromEntries(rawInput.targetIndustries.map((ind, idx) => [ind, 30 - (idx * 5)]))
                    : {
                        technology: 30,
                        professional_services: 25,
                        healthcare: 20,
                        manufacturing: 15,
                        retail: 10,
                    },
                locations: {
                    'North America': 30,
                    'Europe': 25,
                    'APAC': 20,
                    'Other': 10,
                }
            }
        },
        output: {
            webhook: rawInput.webhookUrl || null,
            format: 'csv',
        },
        proxy: rawInput.proxy || {
            useApifyProxy: true,
            apifyProxyGroups: []
        },
        maxConcurrency: rawInput.maxConcurrency || perfSettings.maxConcurrency,
        detailConcurrency: rawInput.maxConcurrency
            ? Math.max(2, Math.floor(rawInput.maxConcurrency * 0.6)) // Scale detail concurrency (60% of main)
            : perfSettings.detailConcurrency,
    };

    console.log('🚀 Starting B2B Lead Generation Actor');
    console.log('Mode:', isBasicMode ? '⚡ BASIC (Fast)' : '🎯 ENRICHED (Slow)');
    console.log('Performance:', `${performancePreset.toUpperCase()} (${input.maxConcurrency} main / ${input.detailConcurrency} detail browsers)`);

    // Warn if concurrency is too high for enriched mode
    if (isEnrichedMode && input.maxConcurrency > 5) {
        console.log('⚠️  WARNING: High concurrency (>5) may cause CPU overload and timeouts in enriched mode.');
        console.log('⚠️  Recommended: Use maxConcurrency 3-5 for best results with email extraction.');
    }

    console.log('Query:', input.searchQueries[0].category, 'in', input.searchQueries[0].location);
    console.log('Max results:', input.searchQueries[0].maxResults);
    console.log('Email extraction:', input.enrichment.extractEmails);
    console.log('Lead scoring:', input.scoring.enableScoring);

    // Initialize statistics
    const stats = {
        totalLeads: 0,
        enrichedLeads: 0,
        emailsFound: 0,
        highQualityLeads: 0, // A+ and A grades
        startTime: new Date().toISOString(),
    };

    // Process single query (new simplified UI only supports one query per run)
    const query = input.searchQueries[0];
    console.log(`📍 Searching: "${query.category}" in "${query.location}"`);

    try {
        // Step 1: Scrape Google Maps with incremental saving
        // Pass callback to enrich and save each lead as it's scraped
        const processAndSaveLead = async (lead) => {
            try {
                let enrichedLead = { ...lead };

                // Add metadata
                enrichedLead.scrapedAt = new Date().toISOString();
                enrichedLead.searchQuery = `${query.category} in ${query.location}`;

                // Email and social media extraction (if enabled and website exists)
                if (input.enrichment?.extractEmails && lead.website) {
                    console.log(`📧 Extracting email and social links from ${lead.website}`);
                    const result = await extractEmailFromWebsite(lead.website);

                    // Set email if found
                    if (result.email && typeof result.email === 'string' && result.email.trim()) {
                        enrichedLead.email = result.email.trim();
                        stats.emailsFound++;
                    } else {
                        enrichedLead.email = null;
                    }

                    // Merge social links from website (prefer website links over Google Maps)
                    if (result.socialLinks) {
                        enrichedLead.socialLinks = {
                            linkedin: result.socialLinks.linkedin || lead.socialLinks?.linkedin || null,
                            facebook: result.socialLinks.facebook || lead.socialLinks?.facebook || null,
                            twitter: result.socialLinks.twitter || lead.socialLinks?.twitter || null,
                            instagram: result.socialLinks.instagram || lead.socialLinks?.instagram || null,
                        };
                    }
                } else {
                    enrichedLead.email = null; // No email extraction enabled or no website
                }

                // Contact validation (if enabled)
                if (input.enrichment?.validateContacts) {
                    if (enrichedLead.email) {
                        enrichedLead.emailValid = validateEmailFormat(enrichedLead.email);
                    }
                    if (enrichedLead.phone) {
                        enrichedLead.phoneValid = true; // Placeholder - implement real validation
                    }
                }

                // Company data enrichment (if enabled)
                if (input.enrichment?.companyData) {
                    // Placeholder - integrate with enrichment API
                    enrichedLead.companyDataNote = 'Company enrichment requires API integration';
                }

                // Technology stack detection (if enabled)
                if (input.enrichment?.techStack && lead.website) {
                    // Placeholder - integrate with tech detection service
                    enrichedLead.techStackNote = 'Tech stack detection requires API integration';
                }

                // Decision maker identification (if enabled)
                if (input.enrichment?.findDecisionMakers) {
                    // Placeholder - integrate with LinkedIn scraper
                    enrichedLead.decisionMakersNote = 'Decision maker search requires LinkedIn integration';
                }

                // Lead scoring (if enabled)
                if (input.scoring?.enableScoring) {
                    const scoreResult = calculateLeadScore(
                        enrichedLead,
                        input.scoring.idealCustomerProfile || {}
                    );
                    enrichedLead.leadScore = scoreResult.score;
                    enrichedLead.leadGrade = scoreResult.grade;
                    enrichedLead.scoreBreakdown = scoreResult.breakdown;

                    // Track high-quality leads
                    if (scoreResult.grade === 'A+' || scoreResult.grade === 'A') {
                        stats.highQualityLeads++;
                    }
                }

                // 💾 SAVE IMMEDIATELY to dataset (incremental saving)
                await Actor.pushData(enrichedLead);
                stats.enrichedLeads++;
                stats.totalLeads++;

                console.log(`💾 Saved lead #${stats.enrichedLeads}: ${enrichedLead.businessName}`);

            } catch (enrichError) {
                console.error(`❌ Failed to enrich lead: ${lead.businessName}`, enrichError.message);

                // Still save the raw lead with error flag
                await Actor.pushData({
                    ...lead,
                    enrichmentError: enrichError.message,
                    scrapedAt: new Date().toISOString(),
                });
                stats.enrichedLeads++;
                stats.totalLeads++;
            }
        };

        // Call scrapeGoogleMaps with incremental callback
        const rawLeads = await scrapeGoogleMaps({
            category: query.category,
            location: query.location,
            maxResults: query.maxResults || 50,
            filters: input.filters || {},
            proxyConfig: input.proxy,
            maxConcurrency: input.maxConcurrency || 5,
            detailConcurrency: input.detailConcurrency, // Separate concurrency for detail pages
            fastMode: input.fastMode, // Use scraping mode setting
            language: input.language || 'en',
            skipClosedPlaces: input.skipClosedPlaces !== false,
            enrichment: input.enrichment || {},
            onLeadScraped: processAndSaveLead, // 🔥 Callback for incremental saving
        });

        console.log(`✅ Scraping complete! Processed ${stats.totalLeads} leads`);

    } catch (queryError) {
        console.error(`❌ Failed to process query: "${query.category}" in "${query.location}"`, queryError.message);
    }

    // Step 3: Finalize and send results
    stats.endTime = new Date().toISOString();
    stats.success = true;

    console.log('🎉 Actor finished successfully!', JSON.stringify(stats, null, 2));

    // Get all data from dataset for webhook/integrations
    const dataset = await Actor.openDataset();
    const { items } = await dataset.getData();

    // Send webhook notification (if configured)
    if (input.output?.webhook) {
        try {
            await sendWebhook(input.output.webhook, {
                status: 'completed',
                stats,
                totalLeads: items.length,
                highQualityLeads: items.filter(l => l.leadGrade === 'A+' || l.leadGrade === 'A').length,
                timestamp: new Date().toISOString(),
                datasetId: process.env.APIFY_DEFAULT_DATASET_ID,
                downloadUrl: `https://api.apify.com/v2/datasets/${process.env.APIFY_DEFAULT_DATASET_ID}/items?format=${input.output?.format || 'csv'}`,
            });
            console.log('✅ Webhook notification sent successfully');
        } catch (webhookError) {
            console.error('❌ Failed to send webhook', webhookError.message);
        }
    }

    // Set final output for Apify platform
    await Actor.setValue('OUTPUT', {
        success: true,
        stats,
        message: `Successfully generated ${stats.totalLeads} leads (${stats.highQualityLeads} high-quality)`,
        downloadFormats: ['csv', 'json', 'xlsx'],
    });

    // Exit successfully
    await Actor.exit();

} catch (error) {
    console.error('💥 FATAL ERROR:', error);
    console.error('Error message:', error.message);
    console.error('Error stack:', error.stack);

    await Actor.setValue('OUTPUT', {
        success: false,
        error: error.message,
        errorDetails: error.stack,
    });

    // Exit with error
    await Actor.exit({ exitCode: 1, statusMessage: `Failed: ${error.message}` });
}

// Simple email format validation
function validateEmailFormat(email) {
    const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    return emailRegex.test(email);
}
