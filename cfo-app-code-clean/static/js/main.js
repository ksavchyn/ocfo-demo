// CFO Control Center - Main JavaScript

// Initialize application when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeApp();
});

function initializeApp() {
    console.log('CFO Control Center initialized');

    // Auto-refresh financial data every 5 minutes
    setInterval(refreshFinancialData, 5 * 60 * 1000);

    // Initialize metric animations
    animateMetrics();
}

function refreshFinancialData() {
    fetch('/api/financial-data')
        .then(response => response.json())
        .then(data => {
            updateMetrics(data.executive_metrics);
            updateSystemStatus(data.system_status);
            console.log('Financial data refreshed');
        })
        .catch(error => {
            console.error('Error refreshing financial data:', error);
        });
}

function updateMetrics(metrics) {
    // Update revenue
    const revenueValue = document.querySelector('.metric-card:nth-child(1) .metric-value');
    if (revenueValue) {
        revenueValue.textContent = `$${metrics.revenue.current}M`;
        revenueValue.style.color = metrics.revenue.variance >= 0 ? 'var(--db-success)' : 'var(--db-error)';
    }

    // Update expenses
    const expensesValue = document.querySelector('.metric-card:nth-child(2) .metric-value');
    if (expensesValue) {
        expensesValue.textContent = `$${metrics.expenses.current}M`;
        expensesValue.style.color = metrics.expenses.variance >= 0 ? 'var(--db-error)' : 'var(--db-success)';
    }

    // Update profit margin
    const profitValue = document.querySelector('.metric-card:nth-child(3) .metric-value');
    if (profitValue) {
        profitValue.textContent = `${metrics.profit_margin.current}%`;
        profitValue.style.color = metrics.profit_margin.variance >= 0 ? 'var(--db-success)' : 'var(--db-error)';
    }

    // Update cash flow
    const cashFlowValue = document.querySelector('.metric-card:nth-child(4) .metric-value');
    if (cashFlowValue) {
        cashFlowValue.textContent = `$${metrics.cash_flow.current}M`;
        cashFlowValue.style.color = metrics.cash_flow.variance >= 0 ? 'var(--db-success)' : 'var(--db-error)';
    }
}

function updateSystemStatus(systems) {
    const statusList = document.querySelector('.status-list');
    if (!statusList) return;

    systems.forEach((system, index) => {
        const statusItem = statusList.children[index];
        if (statusItem) {
            const indicator = statusItem.querySelector('.status-indicator');
            if (indicator) {
                indicator.className = `status-indicator ${system.status}`;
            }

            const details = statusItem.querySelector('.status-details');
            if (details) {
                details.textContent = `${system.last_sync} • ${system.records} records`;
            }
        }
    });
}

function animateMetrics() {
    // Add subtle animation to metric cards on load
    const metricCards = document.querySelectorAll('.metric-card');
    metricCards.forEach((card, index) => {
        setTimeout(() => {
            card.style.opacity = '0';
            card.style.transform = 'translateY(20px)';
            card.style.transition = 'all 0.5s ease-out';

            setTimeout(() => {
                card.style.opacity = '1';
                card.style.transform = 'translateY(0)';
            }, 100);
        }, index * 150);
    });
}

// Utility functions
function formatCurrency(amount) {
    return new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'USD',
        minimumFractionDigits: 0,
        maximumFractionDigits: 1
    }).format(amount);
}

function formatPercentage(value) {
    return `${value >= 0 ? '+' : ''}${value.toFixed(1)}%`;
}

function getTimeAgo(timestamp) {
    const now = new Date();
    const time = new Date(timestamp);
    const diffInMinutes = Math.floor((now - time) / 60000);

    if (diffInMinutes < 1) return 'just now';
    if (diffInMinutes < 60) return `${diffInMinutes} minute${diffInMinutes > 1 ? 's' : ''} ago`;

    const diffInHours = Math.floor(diffInMinutes / 60);
    if (diffInHours < 24) return `${diffInHours} hour${diffInHours > 1 ? 's' : ''} ago`;

    const diffInDays = Math.floor(diffInHours / 24);
    return `${diffInDays} day${diffInDays > 1 ? 's' : ''} ago`;
}