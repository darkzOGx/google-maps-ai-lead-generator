/**
 * Calculate lead quality score based on multiple factors
 * @param {Object} lead - Lead object with all available data
 * @param {Object} icp - Ideal Customer Profile with scoring weights
 * @returns {Object} Score result with total score, grade, and breakdown
 */
export const calculateLeadScore = (lead, icp = {}) => {
    let totalScore = 0;
    const breakdown = {
        dataQuality: 0,
        engagement: 0,
        firmographic: 0,
    };

    // ===== DATA QUALITY SCORING (35 points max) =====
    let dataQualityScore = 0;

    // Valid email (15 points)
    if (lead.email) {
        dataQualityScore += 10;
        if (lead.emailValid) {
            dataQualityScore += 5;
        }
    }

    // Valid phone (10 points)
    if (lead.phone) {
        dataQualityScore += 7;
        if (lead.phoneValid) {
            dataQualityScore += 3;
        }
    }

    // Website exists (5 points)
    if (lead.website) {
        dataQualityScore += 5;
    }

    // Claimed listing (5 points - shows active business)
    if (lead.claimed) {
        dataQualityScore += 5;
    }

    breakdown.dataQuality = dataQualityScore;
    totalScore += dataQualityScore;

    // ===== ENGAGEMENT SIGNALS (25 points max) =====
    let engagementScore = 0;

    // High rating (10 points)
    if (lead.rating) {
        if (lead.rating >= 4.8) {
            engagementScore += 10;
        } else if (lead.rating >= 4.5) {
            engagementScore += 8;
        } else if (lead.rating >= 4.0) {
            engagementScore += 5;
        } else if (lead.rating >= 3.5) {
            engagementScore += 2;
        }
    }

    // Review count (10 points - shows customer engagement)
    if (lead.reviewCount) {
        if (lead.reviewCount >= 100) {
            engagementScore += 10;
        } else if (lead.reviewCount >= 50) {
            engagementScore += 8;
        } else if (lead.reviewCount >= 20) {
            engagementScore += 5;
        } else if (lead.reviewCount >= 10) {
            engagementScore += 3;
        }
    }

    // Social media presence (5 points) - only if social links exist
    let hasSocialLinks = false;
    if (lead.socialLinks) {
        const socialCount = Object.values(lead.socialLinks).filter((link) => link !== null).length;
        if (socialCount > 0) {
            engagementScore += Math.min(socialCount * 1.25, 5); // Max 5 points
            hasSocialLinks = true;
        }
    }

    breakdown.engagement = engagementScore;
    totalScore += engagementScore;

    // ===== FIRMOGRAPHIC FIT (40 points max) =====
    let firmographicScore = 0;

    // Industry match (0-30 points based on ICP)
    if (lead.category && icp.industries) {
        // Handle both array format (old) and object format (new)
        if (Array.isArray(icp.industries)) {
            // Old format: ["Software", "Technology"] - give 30 points if match
            const matchesIndustry = icp.industries.some(ind =>
                lead.category.toLowerCase().includes(ind.toLowerCase())
            );
            if (matchesIndustry) {
                firmographicScore += 30; // Default weight for array format
            }
        } else {
            // New format: { technology: 30, healthcare: 20 }
            const industry = findClosestIndustry(lead.category, icp.industries);
            if (industry) {
                firmographicScore += icp.industries[industry];
            }
        }
    }

    // Location match (0-30 points based on ICP)
    if (lead.address && icp.locations) {
        // Handle both array format (old) and object format (new)
        if (Array.isArray(icp.locations)) {
            // Old format: ["San Francisco", "California"] - give 30 points if match
            const matchesLocation = icp.locations.some(loc =>
                lead.address.toLowerCase().includes(loc.toLowerCase())
            );
            if (matchesLocation) {
                firmographicScore += 30; // Default weight for array format
            }
        } else {
            // New format: { "North America": 30, "Europe": 25 }
            const region = determineRegion(lead.address);
            if (region && icp.locations[region]) {
                firmographicScore += icp.locations[region];
            }
        }
    }

    // Company size (if available from enrichment)
    if (lead.employeeCount && icp.employeeRanges) {
        const range = getEmployeeRange(lead.employeeCount);
        if (icp.employeeRanges[range]) {
            firmographicScore += icp.employeeRanges[range];
        }
    }

    // Cap firmographic score at 40 points max (industry 30 + location 30 can exceed 40)
    firmographicScore = Math.min(firmographicScore, 40);

    breakdown.firmographic = firmographicScore;
    totalScore += firmographicScore;

    // ===== NORMALIZE SCORE IF SOCIAL LINKS NOT AVAILABLE =====
    // If no social links were found, adjust score so leads aren't penalized
    // Max score: 100 (with social) or 95 (without social)
    const maxPossibleScore = hasSocialLinks ? 100 : 95;
    const normalizedScore = hasSocialLinks ? totalScore : (totalScore / maxPossibleScore) * 100;

    // Safety cap: ensure score never exceeds 100
    const cappedScore = Math.min(normalizedScore, 100);

    // ===== CALCULATE FINAL GRADE =====
    // Adjusted thresholds for realistic B2B lead grading
    // Most quality leads with email+phone+website should score A or A+
    let grade;
    if (cappedScore >= 85) {
        grade = 'A+';
    } else if (cappedScore >= 75) {
        grade = 'A';
    } else if (cappedScore >= 65) {
        grade = 'B';
    } else if (cappedScore >= 55) {
        grade = 'C';
    } else if (cappedScore >= 45) {
        grade = 'D';
    } else {
        grade = 'F';
    }

    return {
        score: Math.round(cappedScore),
        grade,
        breakdown,
    };
};

/**
 * Find the closest matching industry from ICP
 */
function findClosestIndustry(category, industries) {
    if (!category || !industries) return null;

    const categoryLower = category.toLowerCase();

    // Direct match
    for (const industry in industries) {
        if (categoryLower.includes(industry.toLowerCase())) {
            return industry;
        }
    }

    // Fuzzy match for common terms
    const industryMap = {
        technology: ['software', 'tech', 'it ', 'computer', 'digital', 'saas', 'app'],
        professional_services: ['consulting', 'legal', 'accounting', 'financial', 'advisory'],
        healthcare: ['medical', 'health', 'clinic', 'hospital', 'doctor', 'dental'],
        manufacturing: ['manufacturing', 'factory', 'industrial', 'production'],
        retail: ['store', 'shop', 'retail', 'boutique', 'market'],
    };

    for (const [industry, keywords] of Object.entries(industryMap)) {
        if (keywords.some((keyword) => categoryLower.includes(keyword))) {
            return industry;
        }
    }

    return null;
}

/**
 * Determine geographic region from address
 */
function determineRegion(address) {
    if (!address) return 'Other';

    const addressLower = address.toLowerCase();

    // North America
    if (
        addressLower.includes('usa') ||
        addressLower.includes('united states') ||
        addressLower.includes('canada') ||
        /\b(al|ak|az|ar|ca|co|ct|de|fl|ga|hi|id|il|in|ia|ks|ky|la|me|md|ma|mi|mn|ms|mo|mt|ne|nv|nh|nj|nm|ny|nc|nd|oh|ok|or|pa|ri|sc|sd|tn|tx|ut|vt|va|wa|wv|wi|wy)\b/.test(
            addressLower
        )
    ) {
        return 'North America';
    }

    // Europe
    if (
        addressLower.includes('uk') ||
        addressLower.includes('united kingdom') ||
        addressLower.includes('europe') ||
        addressLower.includes('germany') ||
        addressLower.includes('france') ||
        addressLower.includes('spain') ||
        addressLower.includes('italy')
    ) {
        return 'Europe';
    }

    // APAC
    if (
        addressLower.includes('asia') ||
        addressLower.includes('china') ||
        addressLower.includes('japan') ||
        addressLower.includes('india') ||
        addressLower.includes('singapore') ||
        addressLower.includes('australia')
    ) {
        return 'APAC';
    }

    return 'Other';
}

/**
 * Get employee range bucket
 */
function getEmployeeRange(count) {
    if (count <= 10) return '1-10';
    if (count <= 50) return '11-50';
    if (count <= 200) return '51-200';
    if (count <= 500) return '201-500';
    return '500+';
}
