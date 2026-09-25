// Custom dropdown component for light mode styling
class CustomDropdown {
    constructor(selectElement) {
        this.select = selectElement;
        this.options = Array.from(selectElement.options);
        this.selectedIndex = selectElement.selectedIndex;
        this.createCustomDropdown();
        this.hideOriginalSelect();
    }

    hideOriginalSelect() {
        this.select.style.display = 'none';
    }

    createCustomDropdown() {
        // Create wrapper
        this.wrapper = document.createElement('div');
        this.wrapper.className = 'custom-dropdown';

        // Create the display button
        this.button = document.createElement('div');
        this.button.className = 'custom-dropdown-button';
        this.button.innerHTML = `
            <span class="custom-dropdown-text">${this.options[this.selectedIndex].text}</span>
            <svg class="custom-dropdown-arrow" width="16" height="16" viewBox="0 0 20 20" fill="none">
                <path d="M5 7.5L10 12.5L15 7.5" stroke="#374151" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
        `;

        // Create dropdown menu
        this.menu = document.createElement('div');
        this.menu.className = 'custom-dropdown-menu';

        // Add options to menu
        this.options.forEach((option, index) => {
            if (index === 0 && option.value === '') return; // Skip the "All" placeholder if it's empty

            const optionDiv = document.createElement('div');
            optionDiv.className = 'custom-dropdown-option';
            optionDiv.textContent = option.text;
            optionDiv.dataset.value = option.value;
            optionDiv.dataset.index = index;

            if (index === this.selectedIndex) {
                optionDiv.classList.add('selected');
            }

            optionDiv.addEventListener('click', () => this.selectOption(index));
            this.menu.appendChild(optionDiv);
        });

        // Add elements to wrapper
        this.wrapper.appendChild(this.button);
        this.wrapper.appendChild(this.menu);

        // Insert after original select
        this.select.parentNode.insertBefore(this.wrapper, this.select.nextSibling);

        // Add event listeners
        this.button.addEventListener('click', () => this.toggleDropdown());

        // Close dropdown when clicking outside
        document.addEventListener('click', (e) => {
            if (!this.wrapper.contains(e.target)) {
                this.closeDropdown();
            }
        });
    }

    toggleDropdown() {
        const isOpen = this.wrapper.classList.contains('open');

        // Close all other dropdowns first
        document.querySelectorAll('.custom-dropdown.open').forEach(dropdown => {
            if (dropdown !== this.wrapper) {
                dropdown.classList.remove('open');
            }
        });

        if (isOpen) {
            this.closeDropdown();
        } else {
            this.openDropdown();
        }
    }

    openDropdown() {
        this.wrapper.classList.add('open');
        this.button.querySelector('.custom-dropdown-arrow').style.transform = 'rotate(180deg)';
    }

    closeDropdown() {
        this.wrapper.classList.remove('open');
        this.button.querySelector('.custom-dropdown-arrow').style.transform = 'rotate(0deg)';
    }

    selectOption(index, opts) {
        // Update visual selection
        this.menu.querySelectorAll('.custom-dropdown-option').forEach(opt => {
            opt.classList.remove('selected');
        });

        const selectedOption = this.menu.querySelector(`[data-index="${index}"]`);
        if (selectedOption) {
            selectedOption.classList.add('selected');
        }

        // Update button text
        this.button.querySelector('.custom-dropdown-text').textContent = this.options[index].text;

        // Update original select
        this.select.selectedIndex = index;
        this.selectedIndex = index;

        // Trigger change event on original select — UNLESS caller asked for
        // silent mode (used by clearAllFilters, which deliberately wants to
        // bulk-reset all dropdowns and then fire applyFilters ONCE at the end,
        // not once per dropdown).
        if (!(opts && opts.silent)) {
            const event = new Event('change', { bubbles: true });
            this.select.dispatchEvent(event);
        }

        // Close dropdown
        this.closeDropdown();
    }

    updateOptions(newOptions) {
        // Clear current menu
        this.menu.innerHTML = '';

        // Update options array
        this.options = Array.from(this.select.options);

        // Rebuild menu
        this.options.forEach((option, index) => {
            if (index === 0 && option.value === '') return; // Skip the "All" placeholder if it's empty

            const optionDiv = document.createElement('div');
            optionDiv.className = 'custom-dropdown-option';
            optionDiv.textContent = option.text;
            optionDiv.dataset.value = option.value;
            optionDiv.dataset.index = index;

            if (index === this.selectedIndex) {
                optionDiv.classList.add('selected');
            }

            optionDiv.addEventListener('click', () => this.selectOption(index));
            this.menu.appendChild(optionDiv);
        });

        // Update button text if selection changed
        if (this.selectedIndex < this.options.length) {
            this.button.querySelector('.custom-dropdown-text').textContent = this.options[this.selectedIndex].text;
        }
    }
}

// Initialize custom dropdowns when page loads
function initializeCustomDropdowns() {
    const selects = document.querySelectorAll('.filter-select');
    const customDropdowns = {};

    selects.forEach(select => {
        customDropdowns[select.id] = new CustomDropdown(select);
    });

    // Store references for cascading filter updates
    window.customDropdowns = customDropdowns;

    // Override the cascading filter update functions to refresh custom dropdowns
    const originalUpdateLocation = window.updateLocationOptions;
    const originalUpdateIndustry = window.updateIndustryOptions;
    const originalUpdateCustomer = window.updateCustomerOptions;

    if (originalUpdateLocation) {
        window.updateLocationOptions = function(region) {
            originalUpdateLocation(region);
            setTimeout(() => {
                if (window.customDropdowns && window.customDropdowns.locationFilter) {
                    window.customDropdowns.locationFilter.updateOptions();
                }
            }, 10);
        };
    }

    if (originalUpdateIndustry) {
        window.updateIndustryOptions = function(practiceArea) {
            originalUpdateIndustry(practiceArea);
            setTimeout(() => {
                if (window.customDropdowns && window.customDropdowns.industryFilter) {
                    window.customDropdowns.industryFilter.updateOptions();
                }
            }, 10);
        };
    }

    if (originalUpdateCustomer) {
        window.updateCustomerOptions = function(industry) {
            originalUpdateCustomer(industry);
            setTimeout(() => {
                if (window.customDropdowns && window.customDropdowns.customerFilter) {
                    window.customDropdowns.customerFilter.updateOptions();
                }
            }, 10);
        };
    }
}

// Initialize when DOM is ready
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeCustomDropdowns);
} else {
    // DOM is already loaded, initialize immediately
    setTimeout(initializeCustomDropdowns, 100);
}