import { Actor } from 'apify';
import { scrapeGoogleMaps } from './scrapers/googleMaps.js';
import { extractEmailFromWebsite } from './scrapers/website.js';
import { calculateLeadScore } from './scoring/leadScore.js';
import { sendWebhook } from './integrations/webhook.js';

// Initialize the Apify actor
await Actor.init();

try {
    // Get input from Apify platform UI
    const input = await Actor.getInput();

    console.log('📥 Input received:', JSON.stringify(input, null, 2));

    // Validate required inputs
    if (!input?.searchQueries || input.searchQueries.length === 0) {
        console.log('❌ No search queries provided! Using default query.');
        // Use default if missing
        input.searchQueries = [
            {
                category: 'software companies',
                location: 'San Francisco, CA',
                maxResults: 10,
            }
        ];
    }

    console.log('🚀 Starting B2B Lead Generation Actor');
    console.log('Queries:', input.searchQueries.length);
    console.log('Enrichment enabled:', input.enrichment?.extractEmails || false);
    console.log('Scoring enabled:', input.scoring?.enableScoring || false);

    // Initialize statistics
    const stats = {
        totalLeads: 0,
        enrichedLeads: 0,
        emailsFound: 0,
        highQualityLeads: 0, // A+ and A grades
        startTime: new Date().toISOString(),
    };

    // Process each search query
    for (const [index, query] of input.searchQueries.entries()) {
        console.log(`📍 Query ${index + 1}/${input.searchQueries.length}: "${query.category}" in "${query.location}"`);

        try {
            // Step 1: Scrape Google Maps
            const rawLeads = await scrapeGoogleMaps({
                category: query.category,
                location: query.location,
                maxResults: query.maxResults || 100,
                filters: input.filters || {},
                proxyConfig: input.proxy,
                maxConcurrency: input.maxConcurrency || 5,
                fastMode: input.fastMode || false,
            });

            console.log(`✅ Found ${rawLeads.length} businesses from Google Maps`);
            stats.totalLeads += rawLeads.length;

            // Step 2: Enrich each lead
            for (const [leadIndex, lead] of rawLeads.entries()) {
                try {
                    let enrichedLead = { ...lead };

                    // Add metadata
                    enrichedLead.scrapedAt = new Date().toISOString();
                    enrichedLead.searchQuery = `${query.category} in ${query.location}`;

                    // Email extraction (if enabled and website exists)
                    if (input.enrichment?.extractEmails && lead.website) {
                        console.log(`📧 Extracting email from ${lead.website}`);
                        const email = await extractEmailFromWebsite(lead.website);
                        if (email) {
                            enrichedLead.email = email;
                            stats.emailsFound++;
                        }
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

                    // Push enriched lead to dataset
                    await Actor.pushData(enrichedLead);
                    stats.enrichedLeads++;

                    // Progress logging every 10 leads
                    if ((leadIndex + 1) % 10 === 0) {
                        console.log(
                            `⏳ Progress: ${leadIndex + 1}/${rawLeads.length} leads processed for this query`
                        );
                    }

                } catch (enrichError) {
                    console.error(`❌ Failed to enrich lead: ${lead.businessName}`, enrichError.message);

                    // Still save the raw lead with error flag
                    await Actor.pushData({
                        ...lead,
                        enrichmentError: enrichError.message,
                        scrapedAt: new Date().toISOString(),
                    });
                    stats.enrichedLeads++;
                }
            }

        } catch (queryError) {
            console.error(`❌ Failed to process query: "${query.category}" in "${query.location}"`, queryError.message);
        }
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
