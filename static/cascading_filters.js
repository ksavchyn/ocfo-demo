// Cascading filter logic for the executive summary page

// Filter mappings from Python (synchronized with filter_mappings.py)
const FILTER_MAPPINGS = {
    region_to_location: {
        "Americas": [
            "Atlanta", "Chicago", "Houston", "Mexico City",
            "New York", "San Francisco", "Sao Paulo", "Toronto",
            "Washington DC"
        ],
        "Asia Pacific": [
            "Bangalore", "Kuala Lumpur", "Manila", "Mumbai",
            "Seoul", "Shanghai", "Singapore", "Sydney", "Tokyo"
        ],
        "EMEA": [
            "Amsterdam", "Dubai", "Dublin", "Frankfurt",
            "London", "Madrid", "Milan", "Paris", "Zurich"
        ]
    },

    location_to_practice_area: {
        // All locations have all practice areas
        _all: [
            "Accounting", "Audit", "Managed Services: Ops",
            "Managed Services: Tech", "Operations",
            "Strategy & Consulting", "Tax", "Technology"
        ]
    },

    practice_area_to_industry: {
        "Accounting": ["FS", "Healthcare", "Manufacturing", "Media", "Other", "Public Sector", "Retail", "Telco"],
        "Audit": ["FS", "Healthcare", "Manufacturing", "Media", "Other", "Public Sector", "Retail", "Telco"],
        "Managed Services: Ops": ["FS", "Healthcare", "Manufacturing", "Other", "Retail", "Telco"],
        "Managed Services: Tech": ["FS", "Manufacturing", "Media", "Other", "Public Sector", "Retail"],
        "Operations": ["FS", "Healthcare", "Manufacturing", "Media", "Other", "Public Sector", "Retail", "Telco"],
        "Strategy & Consulting": ["FS", "Healthcare", "Manufacturing", "Media", "Other", "Public Sector", "Retail", "Telco"],
        "Tax": ["FS", "Healthcare", "Manufacturing", "Media", "Other", "Public Sector", "Retail", "Telco"],
        "Technology": ["FS", "Healthcare", "Manufacturing", "Media", "Other", "Public Sector", "Retail", "Telco"]
    },

    industry_to_customer: {
        "FS": [
            "AIG", "American Express", "Bank of America", "BlackRock",
            "Charles Schwab", "Citigroup", "Fidelity", "Goldman Sachs",
            "JPMorgan Chase", "Mastercard", "MetLife", "Morgan Stanley",
            "Prudential", "Visa", "Wells Fargo"
        ],
        "Healthcare": [
            "Abbott", "Anthem", "CMS", "CVS Health", "City of New York",
            "Johnson & Johnson", "Merck", "NHS", "Pfizer",
            "State of California", "State of Texas", "US Dept of Defense",
            "US Dept of Veterans Affairs", "UnitedHealth", "WHO"
        ],
        "Manufacturing": [
            "3M", "Amazon", "BMW", "Boeing", "Caterpillar", "Coca-Cola",
            "Ford", "General Motors", "Honeywell", "Nike", "PepsiCo",
            "Procter & Gamble", "Toyota", "Unilever", "Walmart"
        ],
        "Media": [
            "AT&T", "Adobe", "Cisco", "Comcast", "Disney", "Google",
            "Intel", "Meta", "Microsoft", "Netflix", "Oracle",
            "Qualcomm", "SAP", "Salesforce", "Verizon"
        ],
        "Other": [
            "BHP", "BP", "Chevron", "ConocoPhillips", "Duke Energy",
            "Enbridge", "ExxonMobil", "Newmont", "NextEra Energy",
            "Rio Tinto", "Shell", "Southern Company", "TotalEnergies",
            "Veolia", "Waste Management"
        ],
        "Public Sector": [
            "Abbott", "Anthem", "CMS", "CVS Health", "City of New York",
            "Johnson & Johnson", "Merck", "NHS", "Pfizer",
            "State of California", "State of Texas", "US Dept of Defense",
            "US Dept of Veterans Affairs", "UnitedHealth", "WHO"
        ],
        "Retail": [
            "BHP", "BP", "Chevron", "ConocoPhillips", "Duke Energy",
            "Enbridge", "ExxonMobil", "Newmont", "NextEra Energy",
            "Rio Tinto", "Shell", "Southern Company", "TotalEnergies",
            "Veolia", "Waste Management"
        ],
        "Telco": [
            "AT&T", "Adobe", "Cisco", "Comcast", "Disney", "Google",
            "Intel", "Meta", "Microsoft", "Netflix", "Oracle",
            "Qualcomm", "SAP", "Salesforce", "Verizon"
        ]
    }
};

// Called from inline onchange BEFORE applyFilters(). Synchronously runs
// the cascade (option rebuild + downstream filter reset + custom-dropdown
// UI sync) so by the time applyFilters() reads filter values, every
// downstream filter that was invalidated by the change has already been
// reset to "All". This fixes the Region=Americas + Location=Amsterdam
// bug where the inline `applyFilters()` was reading stale values.
window.cascadeAndApply = function(changedId) {
    const regionFilter = document.getElementById('regionFilter');
    const locationFilter = document.getElementById('locationFilter');
    const practiceAreaFilter = document.getElementById('practiceAreaFilter');
    const industryFilter = document.getElementById('industryFilter');
    const customerFilter = document.getElementById('customerFilter');

    if (changedId === 'regionFilter' && regionFilter) {
        if (typeof updateLocationOptions === 'function') {
            updateLocationOptions(regionFilter.value);
        }
        if (locationFilter) { locationFilter.selectedIndex = 0; syncCustomDropdownUI(locationFilter); }
        if (practiceAreaFilter) { practiceAreaFilter.selectedIndex = 0; syncCustomDropdownUI(practiceAreaFilter); }
        if (industryFilter) { industryFilter.selectedIndex = 0; syncCustomDropdownUI(industryFilter); }
        if (customerFilter) { customerFilter.selectedIndex = 0; syncCustomDropdownUI(customerFilter); }
    } else if (changedId === 'practiceAreaFilter' && practiceAreaFilter) {
        if (typeof updateIndustryOptions === 'function') {
            updateIndustryOptions(practiceAreaFilter.value);
        }
        if (industryFilter) { industryFilter.selectedIndex = 0; syncCustomDropdownUI(industryFilter); }
        if (customerFilter) { customerFilter.selectedIndex = 0; syncCustomDropdownUI(customerFilter); }
    } else if (changedId === 'industryFilter' && industryFilter) {
        if (typeof updateCustomerOptions === 'function') {
            updateCustomerOptions(industryFilter.value);
        }
        if (customerFilter) { customerFilter.selectedIndex = 0; syncCustomDropdownUI(customerFilter); }
    }
    // locationFilter and customerFilter changes don't cascade.

    if (typeof window.applyFilters === 'function') {
        window.applyFilters();
    }
};


// Sync the custom-dropdown UI to whatever the underlying <select> currently
// shows. Called after we reset selectedIndex on a downstream filter so the
// visible button text doesn't keep showing a stale (now-invalid) value.
function syncCustomDropdownUI(selectEl) {
    if (!selectEl || !window.customDropdowns) return;
    const dropdown = window.customDropdowns[selectEl.id];
    if (!dropdown) return;
    dropdown.selectedIndex = selectEl.selectedIndex;
    const opts = Array.from(selectEl.options);
    const idx = selectEl.selectedIndex;
    if (idx >= 0 && idx < opts.length) {
        const textEl = dropdown.button && dropdown.button.querySelector('.custom-dropdown-text');
        if (textEl) textEl.textContent = opts[idx].text;
    }
    // Reflect the new options list in the custom dropdown menu too.
    if (typeof dropdown.updateOptions === 'function') {
        dropdown.updateOptions();
    }
}

function initializeCascadingFilters() {
    // Get all filter elements by ID
    const regionFilter = document.getElementById('regionFilter');
    const locationFilter = document.getElementById('locationFilter');
    const practiceAreaFilter = document.getElementById('practiceAreaFilter');
    const industryFilter = document.getElementById('industryFilter');
    const customerFilter = document.getElementById('customerFilter');

    if (!regionFilter || !locationFilter || !practiceAreaFilter || !industryFilter || !customerFilter) {
        console.log('[CASCADING] Not all filters found, trying to find available filters');
        console.log('[CASCADING] Found:', {
            region: !!regionFilter,
            location: !!locationFilter,
            practice: !!practiceAreaFilter,
            industry: !!industryFilter,
            customer: !!customerFilter
        });
        return;
    }

    console.log('[CASCADING] All filters found, initializing cascading logic');

    // Update location options when region changes.
    // NOTE: locationFilter selectedIndex is reset here so the underlying
    // select.value goes back to "All" — previously this was missed, leading
    // to invalid combos like Region=Americas + Location=Amsterdam persisting
    // after the user changed Region.
    // Re-entry guard: when a cascade handler dispatches a `change` event on a
    // downstream filter, we don't want that synthetic event to ALSO fire
    // applyFilters mid-cascade. The outer (user-initiated) cascade is the only
    // one that should fire the final applyFilters() at the end.
    let _cascading = false;

    regionFilter.addEventListener('change', function() {
        if (_cascading) return;
        _cascading = true;
        console.log('[CASCADING] Region changed to:', this.value);
        updateLocationOptions(this.value);
        locationFilter.selectedIndex = 0;
        practiceAreaFilter.selectedIndex = 0;
        industryFilter.selectedIndex = 0;
        customerFilter.selectedIndex = 0;
        syncCustomDropdownUI(locationFilter);
        syncCustomDropdownUI(practiceAreaFilter);
        syncCustomDropdownUI(industryFilter);
        syncCustomDropdownUI(customerFilter);
        // No applyFilters() here — inline onchange already called it with
        // freshly-reset filter values now that we ran first via the deferred
        // pattern in cfo_landing.html.
        _cascading = false;
    });

    practiceAreaFilter.addEventListener('change', function() {
        if (_cascading) return;
        _cascading = true;
        updateIndustryOptions(this.value);
        industryFilter.selectedIndex = 0;
        customerFilter.selectedIndex = 0;
        syncCustomDropdownUI(industryFilter);
        syncCustomDropdownUI(customerFilter);
        _cascading = false;
    });

    industryFilter.addEventListener('change', function() {
        if (_cascading) return;
        _cascading = true;
        updateCustomerOptions(this.value);
        customerFilter.selectedIndex = 0;
        syncCustomDropdownUI(customerFilter);
        _cascading = false;
    });

    // Initialize on page load if region has a value
    if (regionFilter.value && regionFilter.value !== '') {
        updateLocationOptions(regionFilter.value);
    }
    if (practiceAreaFilter.value && practiceAreaFilter.value !== '') {
        updateIndustryOptions(practiceAreaFilter.value);
    }
    if (industryFilter.value && industryFilter.value !== '') {
        updateCustomerOptions(industryFilter.value);
    }
}

function updateLocationOptions(region) {
    const locationFilter = document.getElementById('locationFilter');
    if (!locationFilter) return;

    const currentValue = locationFilter.value;

    // Clear all options except the first (All/placeholder)
    while (locationFilter.options.length > 1) {
        locationFilter.remove(1);
    }

    console.log('[CASCADING] Updating locations for region:', region);

    if (region && region !== '' && region !== 'All' && FILTER_MAPPINGS.region_to_location[region]) {
        const validLocations = FILTER_MAPPINGS.region_to_location[region];
        console.log('[CASCADING] Valid locations for', region, ':', validLocations);

        validLocations.forEach(location => {
            const option = document.createElement('option');
            option.value = location;
            option.textContent = location;
            locationFilter.appendChild(option);
        });

        // Restore previous value if it's still valid
        if (validLocations.includes(currentValue)) {
            locationFilter.value = currentValue;
        }
    } else {
        // If no region selected, show all locations
        const allLocations = new Set();
        Object.values(FILTER_MAPPINGS.region_to_location).forEach(locations => {
            locations.forEach(loc => allLocations.add(loc));
        });

        Array.from(allLocations).sort().forEach(location => {
            const option = document.createElement('option');
            option.value = location;
            option.textContent = location;
            locationFilter.appendChild(option);
        });
    }
}

function updateIndustryOptions(practiceArea) {
    const industryFilter = document.getElementById('industryFilter');
    if (!industryFilter) return;

    const currentValue = industryFilter.value;

    // Clear all options except the first (All/placeholder)
    while (industryFilter.options.length > 1) {
        industryFilter.remove(1);
    }

    if (practiceArea && practiceArea !== '' && FILTER_MAPPINGS.practice_area_to_industry[practiceArea]) {
        const validIndustries = FILTER_MAPPINGS.practice_area_to_industry[practiceArea];

        validIndustries.forEach(industry => {
            const option = document.createElement('option');
            option.value = industry;
            option.textContent = industry;
            industryFilter.appendChild(option);
        });

        // Restore previous value if it's still valid
        if (validIndustries.includes(currentValue)) {
            industryFilter.value = currentValue;
        }
    } else {
        // If no practice area selected, show all industries
        const allIndustries = new Set();
        Object.values(FILTER_MAPPINGS.practice_area_to_industry).forEach(industries => {
            industries.forEach(ind => allIndustries.add(ind));
        });

        Array.from(allIndustries).sort().forEach(industry => {
            const option = document.createElement('option');
            option.value = industry;
            option.textContent = industry;
            industryFilter.appendChild(option);
        });
    }
}

function updateCustomerOptions(industry) {
    const customerFilter = document.getElementById('customerFilter');
    if (!customerFilter) return;

    const currentValue = customerFilter.value;

    // Clear all options except the first (All/placeholder)
    while (customerFilter.options.length > 1) {
        customerFilter.remove(1);
    }

    if (industry && industry !== '' && FILTER_MAPPINGS.industry_to_customer[industry]) {
        const validCustomers = FILTER_MAPPINGS.industry_to_customer[industry];

        validCustomers.forEach(customer => {
            const option = document.createElement('option');
            option.value = customer;
            option.textContent = customer;
            customerFilter.appendChild(option);
        });

        // Restore previous value if it's still valid
        if (validCustomers.includes(currentValue)) {
            customerFilter.value = currentValue;
        }
    } else {
        // If no industry selected, show all customers
        const allCustomers = new Set();
        Object.values(FILTER_MAPPINGS.industry_to_customer).forEach(customers => {
            customers.forEach(cust => allCustomers.add(cust));
        });

        Array.from(allCustomers).sort().forEach(customer => {
            const option = document.createElement('option');
            option.value = customer;
            option.textContent = customer;
            customerFilter.appendChild(option);
        });
    }
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeCascadingFilters);
} else {
    initializeCascadingFilters();
}