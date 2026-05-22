# CFO Demo Landing Page - Flask Application
from flask import Flask, render_template, jsonify, request
import logging
import os
import random
from datetime import datetime, timedelta
import requests
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
app_logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'cfo-demo-secret-key')

# Log startup
app_logger.info("=" * 50)
app_logger.info("CFO APP STARTING - VERSION 4.0")
app_logger.info("=" * 50)

# Initialize Databricks WorkspaceClient once
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    app_logger.info("SUCCESS: Databricks WorkspaceClient initialized")
    print("INFO: Databricks WorkspaceClient initialized successfully")
    # Test that we can list warehouses to verify connection
    try:
        warehouses = list(w.warehouses.list())
        print(f"INFO: Found {len(warehouses)} SQL warehouses")
    except Exception as test_e:
        print(f"WARNING: WorkspaceClient initialized but can't list warehouses: {test_e}")
except Exception as e:
    print(f"WARNING: Could not initialize WorkspaceClient: {e}")
    print(f"WARNING: Will use SQL ai_query fallback for Claude calls")
    w = None

def execute_sql_query(query):
    """Execute a SQL query using Databricks SDK"""
    if not w:
        print("ERROR: WorkspaceClient not initialized")
        return None

    try:
        # Get SQL warehouse ID - this should be set in environment
        # You can find this in Databricks SQL > SQL Warehouses > Your Warehouse > Connection Details
        warehouse_id = os.environ.get('SQL_WAREHOUSE_ID')

        if not warehouse_id:
            # Try to get the first available warehouse
            warehouses = list(w.warehouses.list())
            if warehouses:
                warehouse_id = warehouses[0].id
                print(f"INFO: Using warehouse ID: {warehouse_id}")
            else:
                print("ERROR: No SQL warehouse ID found")
                return None

        # Execute the query using SDK
        result = w.sql.execute(
            warehouse_id=warehouse_id,
            statement=query,
            wait_timeout="10s"
        )

        # Convert result to list format for backward compatibility
        data = []
        if result and result.result and result.result.data_array:
            data = result.result.data_array

        return data
    except Exception as e:
        print(f"Error executing SQL query via SDK: {e}")
        print(f"Query was: {query[:200]}...")
        return None

# Add CSP headers to allow iframe embedding from Databricks
@app.after_request
def after_request(response):
    """Add headers to allow iframe embedding from Databricks"""
    # Allow embedding from Databricks workspace
    response.headers['Content-Security-Policy'] = "frame-ancestors 'self' https://*.cloud.databricks.com"
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    return response

# Mock data for CFO Demo
def get_mock_financial_data():
    """Generate mock financial data for the CFO Control Center"""

    # Current date for reporting
    today = datetime.now()

    # Executive Summary Metrics
    executive_metrics = {
        'revenue': {
            'current': 45.7,  # millions
            'target': 48.2,
            'variance': -5.2,
            'trend': 'down'
        },
        'expenses': {
            'current': 38.2,
            'target': 36.5,
            'variance': 4.7,
            'trend': 'up'
        },
        'profit_margin': {
            'current': 16.4,
            'target': 24.3,
            'variance': -32.5,
            'trend': 'down'
        },
        'cash_flow': {
            'current': 7.5,
            'target': 11.7,
            'variance': -35.9,
            'trend': 'down'
        }
    }

    # Daily insights vs monthly batch comparison
    daily_insights = {
        'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'data_freshness': 'Real-time',
        'sap_sync_status': 'Synchronized',
        'reconciliation_status': 97.8  # percentage reconciled
    }

    # System integration status
    system_status = [
        {'name': 'SAP S/4HANA', 'status': 'connected', 'last_sync': '2 minutes ago', 'records': '1.2M'},
        {'name': 'Salesforce', 'status': 'connected', 'last_sync': '5 minutes ago', 'records': '847K'},
        {'name': 'Workday', 'status': 'connected', 'last_sync': '1 minute ago', 'records': '156K'},
        {'name': 'BI Systems', 'status': 'connected', 'last_sync': '3 minutes ago', 'records': '2.1M'}
    ]

    # Recent financial activities
    recent_activities = [
        {'type': 'expense', 'description': 'New contractor onboarded - Project Alpha', 'amount': -15000, 'time': '10 minutes ago'},
        {'type': 'revenue', 'description': 'Client payment received - ABC Corp', 'amount': 125000, 'time': '25 minutes ago'},
        {'type': 'expense', 'description': 'Equipment purchase - Databricks licenses', 'amount': -8500, 'time': '1 hour ago'},
        {'type': 'revenue', 'description': 'Milestone payment - Project Beta', 'amount': 75000, 'time': '2 hours ago'},
    ]

    # Top concerns for CFO attention
    concerns = [
        {'priority': 'high', 'title': 'Project Alpha Over Budget', 'description': 'Current expenses exceed budget by 12%', 'impact': '$180K'},
        {'priority': 'medium', 'title': 'Delayed Client Payments', 'description': '3 clients with overdue invoices', 'impact': '$420K'},
        {'priority': 'low', 'title': 'Contractor Rate Increases', 'description': 'Average hourly rate up 8% this quarter', 'impact': '$65K'}
    ]

    return {
        'executive_metrics': executive_metrics,
        'daily_insights': daily_insights,
        'system_status': system_status,
        'recent_activities': recent_activities,
        'concerns': concerns
    }

# Routes
@app.route('/')
def landing_page():
    """CFO Control Center Landing Page"""
    financial_data = get_mock_financial_data()
    return render_template('cfo_landing.html', data=financial_data)

@app.route('/api/chat', methods=['POST'])
def chat_endpoint():
    """Mock chat endpoint for CFO queries"""
    data = request.get_json()
    user_message = data.get('message', '')

    # Mock chat responses based on common CFO questions
    mock_responses = {
        'revenue': 'Current revenue is $45.7M, which is 5.2% below target. The main contributing factors are delayed project deliveries in Q4.',
        'expenses': 'Total expenses are $38.2M, running 4.7% over budget primarily due to increased contractor costs and equipment purchases.',
        'cash flow': 'Cash flow is at $7.5M, down 35.9% from target. I recommend reviewing the accounts receivable aging report.',
        'budget': 'We are currently tracking 8.2% over budget across all departments. The largest variances are in IT and Professional Services.',
        'projects': 'Project Alpha is 12% over budget at $180K variance. Project Beta is on track and within 2% of budget.',
        'default': 'I can help you analyze financial data, review budgets, track expenses, and provide insights on revenue trends. What would you like to know?'
    }

    # Simple keyword matching for demo
    response_key = 'default'
    for key in mock_responses.keys():
        if key in user_message.lower():
            response_key = key
            break

    response = {
        'message': mock_responses[response_key],
        'timestamp': datetime.now().isoformat(),
        'suggestions': [
            'Show me the budget variance report',
            'What are the top financial concerns?',
            'How is our cash flow trending?',
            'Update me on Project Alpha status'
        ]
    }

    return jsonify(response)

@app.route('/api/financial-data')
def get_financial_data():
    """API endpoint to get current financial data"""
    return jsonify(get_mock_financial_data())

@app.route('/api/unpaid-invoices')
def get_unpaid_invoices():
    """API endpoint to get real unpaid invoices from gold_receivables_wip_aging"""
    query = """
    SELECT
        client_name,
        project_name,
        unbilled_amount,
        avg_days_unbilled,
        lead_partner_name,
        action_item,
        collection_priority_score
    FROM main.cfo.gold_receivables_wip_aging
    WHERE collection_priority_score >= 80
    ORDER BY collection_priority_score DESC, unbilled_amount DESC
    LIMIT 5
    """

    data = execute_sql_query(query)

    if data:
        # Format data for frontend
        invoices = []
        for row in data:
            invoices.append({
                'client': row[0],
                'project': row[1],
                'amount': float(row[2]) if row[2] else 0,
                'days_overdue': int(row[3]) if row[3] else 0,
                'partner': row[4] if row[4] else 'Unassigned',
                'action': row[5],
                'priority': int(row[6]) if row[6] else 0
            })
        return jsonify({'status': 'success', 'data': invoices})
    else:
        # Return mock data as fallback
        return jsonify({
            'status': 'mock',
            'data': [
                {'client': 'Acme Corp', 'project': 'Q4 Project', 'amount': 150000, 'days_overdue': 100, 'partner': 'J. Smith', 'action': 'URGENT: Invoice immediately', 'priority': 100},
                {'client': 'TechSolutions', 'project': 'Phase 2', 'amount': 125000, 'days_overdue': 75, 'partner': 'S. Lee', 'action': 'HIGH: Follow up required', 'priority': 95},
                {'client': 'Global Dynamics', 'project': 'Service Fee', 'amount': 98500, 'days_overdue': 50, 'partner': 'M. Davis', 'action': 'MEDIUM: Review status', 'priority': 85},
                {'client': 'Innovatech', 'project': 'Consulting', 'amount': 75250, 'days_overdue': 35, 'partner': 'R. Chen', 'action': 'Monitor', 'priority': 80},
                {'client': 'Apex Partners', 'project': 'Audit', 'amount': 62000, 'days_overdue': 23, 'partner': 'K. Gupta', 'action': 'Monitor', 'priority': 80}
            ]
        })

@app.route('/api/te-outliers')
def get_te_outliers():
    """API endpoint to get real T&E outliers from gold_te_contract_audit"""
    query = """
    SELECT
        client_name,
        actual_te_spend,
        total_contract_value,
        te_utilization_pct,
        lead_partner_name,
        project_name,
        action_item
    FROM main.cfo.gold_te_contract_audit
    WHERE te_utilization_pct > 6
    ORDER BY te_utilization_pct DESC
    LIMIT 5
    """

    data = execute_sql_query(query)

    if data:
        # Format data for frontend
        outliers = []
        for row in data:
            contract_value = float(row[2]) if row[2] else 1
            te_spend = float(row[1]) if row[1] else 0
            utilization_pct = float(row[3]) if row[3] else (te_spend / contract_value * 100 if contract_value > 0 else 0)

            outliers.append({
                'client': row[0],
                'expense': te_spend,
                'contract_value': contract_value,
                'percentage': round(utilization_pct, 1),
                'partner': row[4] if row[4] else 'Unassigned',
                'project': row[5],
                'action': row[6]
            })
        return jsonify({'status': 'success', 'data': outliers})
    else:
        # Return mock data as fallback
        return jsonify({
            'status': 'mock',
            'data': [
                {'client': 'Acme Corp', 'expense': 8500, 'contract_value': 100000, 'percentage': 8.5, 'partner': 'J. Smith', 'project': 'Digital Transformation', 'action': 'Review expenses'},
                {'client': 'TechSolutions', 'expense': 12200, 'contract_value': 156410, 'percentage': 7.8, 'partner': 'S. Lee', 'project': 'Cloud Migration', 'action': 'Review expenses'},
                {'client': 'Global Dynamics', 'expense': 6100, 'contract_value': 84722, 'percentage': 7.2, 'partner': 'M. Davis', 'project': 'Strategy Consulting', 'action': 'Monitor'},
                {'client': 'Innovatech', 'expense': 4800, 'contract_value': 69565, 'percentage': 6.9, 'partner': 'R. Chen', 'project': 'Process Optimization', 'action': 'Monitor'},
                {'client': 'Apex Partners', 'expense': 3950, 'contract_value': 61719, 'percentage': 6.4, 'partner': 'K. Gupta', 'project': 'Risk Assessment', 'action': 'Monitor'}
            ]
        })

@app.route('/finance-deepdive')
def finance_deepdive():
    """Finance Deep-Dive page with embedded Lakeview dashboard"""
    # Dashboard URL with workspace and dashboard ID
    dashboard_url = "https://e2-demo-west.cloud.databricks.com/dashboardsv3/01f114d3e8ec125c819eb129ba4f12c3/published?o=2556758628403379"

    # Current timestamp for "last updated" display
    current_time = datetime.now()

    return render_template('finance.html',
                         dashboard_url=dashboard_url,
                         current_time=current_time)

@app.route('/admin-deepdive')
def admin_deepdive():
    """Admin Deep-Dive page with embedded Lakeview dashboard"""
    # Dashboard URL with workspace and dashboard ID
    dashboard_url = "https://e2-demo-west.cloud.databricks.com/dashboardsv3/01f118dca4c511eeb07c075fc6c8dfdb/published?o=2556758628403379"

    # Current timestamp for "last updated" display
    current_time = datetime.now()

    return render_template('admin.html',
                         dashboard_url=dashboard_url,
                         current_time=current_time)

def call_claude_api(prompt):
    """Call the Databricks Claude endpoint using SQL ai_query function"""
    print(f"DEBUG: [v3-SQL] Attempting to call Claude API via SQL ai_query...")
    print(f"DEBUG: This is the NEW version using SQL, not HTTP")

    # Escape single quotes in the prompt for SQL
    escaped_prompt = prompt.replace("'", "''")

    # Use SQL ai_query which works in Databricks environment
    query = f"""
    SELECT ai_query(
        'databricks-claude-opus-4-6',
        '{escaped_prompt}'
    ) as response
    """

    try:
        print(f"DEBUG: Executing SQL ai_query...")
        result = execute_sql_query(query)

        if result and len(result) > 0:
            # Result is in format [[response_text]]
            ai_response = result[0][0] if isinstance(result[0], list) else result[0]
            print(f"DEBUG: Successfully generated email with AI via SQL ai_query")
            print(f"DEBUG: Response length: {len(str(ai_response))} chars")
            return ai_response
        else:
            print(f"ERROR: No result from ai_query")
            # Try SDK method as fallback
            return call_claude_api_sdk(prompt)

    except Exception as e:
        print(f"ERROR: Exception calling Claude via SQL: {type(e).__name__}: {e}")
        # Try SDK method as fallback
        return call_claude_api_sdk(prompt)

def call_claude_api_sdk(prompt):
    """Fallback method using SDK serving endpoints"""
    if not w:
        print("ERROR: WorkspaceClient not initialized for SDK fallback")
        return None

    print(f"DEBUG: Trying SDK serving endpoint as fallback...")

    try:
        response = w.serving_endpoints.query(
            name="databricks-claude-opus-4-6",
            messages=[
                ChatMessage(
                    role=ChatMessageRole.USER,
                    content=prompt
                )
            ],
            max_tokens=1000,
            temperature=0.7
        )

        if response.choices and len(response.choices) > 0:
            generated_text = response.choices[0].message.content
            print(f"DEBUG: Successfully generated email with AI via SDK")
            return generated_text
        else:
            print(f"ERROR: No choices in Claude SDK response")
            return None

    except Exception as e:
        print(f"ERROR: SDK fallback also failed: {e}")
        return None

def build_email_prompt(email_type, context):
    """Build the prompt for email generation based on type and context"""

    # Add variation to prompts to ensure different outputs
    import random
    import time

    # Use timestamp to ensure uniqueness
    timestamp = int(time.time())
    rewrite_count = context.get('rewrite_count', 0)
    variations = ['friendly', 'professional', 'urgent', 'understanding', 'direct', 'empathetic', 'firm', 'collaborative']
    variation = variations[rewrite_count % len(variations)]

    print(f"BUILD_PROMPT - Rewrite #{rewrite_count}, Variation: {variation}, Timestamp: {timestamp}")

    if email_type == 'collection':
        prompt = f"""[Email Version {rewrite_count + 1} - Timestamp {timestamp}]

You are a senior partner at Perry Homes helping with collections. Generate a professional yet friendly email for a collection nudge.

Context:
- Client: {context.get('client')}
- Project: {context.get('project')}
- Invoice: {context.get('invoice')}
- Partner responsible: {context.get('partner')}
- Invoice amount: ${context.get('amount', 0):,.2f}
- Days overdue: {context.get('daysOverdue')} days
- Priority: {context.get('priority', 'HIGH')}
- Action required: {context.get('action', 'Invoice immediately')}
- Tone variation: {variation}
- Version: {rewrite_count + 1}

Generate an email that:
1. Is professional and maintains good client relationships
2. Clearly states the overdue invoice details
3. Provides a direct link/call-to-action to approve unbilled WIP or follow up on payment
4. Mentions the financial impact and importance of timely payment
5. Offers assistance if there are any issues or questions

CRITICAL INSTRUCTIONS:
- This is version {rewrite_count + 1} of the email
- You MUST use a {variation} tone
- Generate COMPLETELY DIFFERENT wording from previous versions
- Use different opening, different structure, different closing
- Be creative while maintaining professionalism
- DO NOT repeat phrases from earlier versions

The email should be formatted with:
- A clear subject line
- Professional greeting
- Body with key points
- Call to action
- Professional closing

Return the email in this JSON format:
{{
    "subject": "Subject line here",
    "body": "Email body here"
}}"""

    else:  # notification type
        prompt = f"""[Email Version {rewrite_count + 1} - Timestamp {timestamp}]

You are a senior partner at Perry Homes helping with expense management. Generate a professional notification email about T&E expenses exceeding thresholds.

Context:
- Client: {context.get('client')}
- Partner responsible: {context.get('partner')}
- Expense amount: ${context.get('expense', 0):,.2f}
- Percentage of contract: {context.get('percentage')}%
- Threshold: 6% of contract value
- Tone variation: {variation}
- Version: {rewrite_count + 1}

Generate an email that:
1. Professionally addresses the T&E overage
2. Provides specific details about the expenses
3. Requests review and justification if appropriate
4. Mentions compliance and budget considerations
5. Offers to discuss if there are special circumstances

CRITICAL INSTRUCTIONS:
- This is version {rewrite_count + 1} of the email
- You MUST use a {variation} tone
- Generate COMPLETELY DIFFERENT wording from previous versions
- Use different opening, different structure, different closing
- Be creative while maintaining professionalism
- DO NOT repeat phrases from earlier versions

The email should be formatted with:
- A clear subject line
- Professional greeting
- Body with key points
- Request for action
- Professional closing

Return the email in this JSON format:
{{
    "subject": "Subject line here",
    "body": "Email body here"
}}"""

    return prompt

@app.route('/api/generate-email', methods=['POST'])
def generate_email():
    """API endpoint to generate email using Claude AI"""
    try:
        data = request.json
        email_type = data.get('type', 'collection')
        context = data.get('context', {})

        # Track rewrite count
        rewrite_count = data.get('rewrite_count', 0)
        context['rewrite_count'] = rewrite_count

        print(f"GENERATE EMAIL CALLED - Type: {email_type}, Rewrite count: {rewrite_count}")

        # Build the prompt for Claude
        prompt = build_email_prompt(email_type, context)
        print(f"PROMPT BUILT - Length: {len(prompt)} chars")

        # Call Claude API
        ai_response = call_claude_api(prompt)
        print(f"AI RESPONSE RECEIVED: {bool(ai_response)}")

        if ai_response:
            print(f"AI RESPONSE SUCCESS - Length: {len(ai_response)} chars")
            try:
                # Try to parse as JSON first
                if '{' in ai_response and '}' in ai_response:
                    # Extract JSON from the response
                    start = ai_response.find('{')
                    end = ai_response.rfind('}') + 1
                    json_str = ai_response[start:end]
                    email_data = json.loads(json_str)
                    print(f"PARSED JSON - Subject: {email_data.get('subject', 'N/A')[:50]}...")
                    return jsonify(email_data)
                else:
                    # If not JSON, create a structured response
                    return jsonify({
                        'subject': f"Action Required: {'Collection' if email_type == 'collection' else 'T&E Review'} - {context.get('invoice', context.get('client', 'Client'))}",
                        'body': ai_response
                    })
            except json.JSONDecodeError:
                # If JSON parsing fails, use the response as the body
                return jsonify({
                    'subject': f"Action Required: {'Collection' if email_type == 'collection' else 'T&E Review'} - {context.get('invoice', context.get('client', 'Client'))}",
                    'body': ai_response
                })
        else:
            # Fallback to template if AI fails
            if email_type == 'collection':
                return jsonify({
                    'subject': f"Payment Reminder: {context.get('invoice', 'Invoice')} - {context.get('daysOverdue', 0)} Days Overdue",
                    'body': f"""Dear {context.get('partner', 'Partner')},

I hope this message finds you well. I'm writing to follow up on an outstanding invoice that requires your attention.

Invoice Details:
• Invoice: {context.get('invoice', 'N/A')}
• Amount: ${context.get('amount', 0):,.2f}
• Days Overdue: {context.get('daysOverdue', 0)} days
• Original Due Date: Please review in system

This invoice has exceeded our standard payment terms, and we would appreciate your immediate attention to this matter. Timely payment helps us maintain our operational efficiency and continue providing excellent service to all our clients.

Action Required:
Please click here to review and approve any unbilled WIP or arrange for immediate payment of the overdue amount.

If there are any issues or concerns preventing payment, please don't hesitate to reach out so we can work together on a resolution.

Thank you for your prompt attention to this matter.

Best regards,
Priya Patel
Senior Partner
Finance & Admin Control Center
Perry Homes"""
                })
            else:
                return jsonify({
                    'subject': f"T&E Expense Review Required: {context.get('client', 'Client')} - {context.get('percentage', 0)}% of Contract",
                    'body': f"""Dear {context.get('partner', 'Partner')},

I'm reaching out regarding travel and entertainment expenses that require your review.

Expense Details:
• Client: {context.get('client', 'N/A')}
• T&E Amount: ${context.get('expense', 0):,.2f}
• Percentage of Contract: {context.get('percentage', 0)}%
• Threshold: 6% of contract value

The expenses for this engagement have exceeded our standard threshold, triggering this automated review. This is part of our regular monitoring to ensure project profitability and compliance with our expense policies.

Action Required:
Please review the detailed expense breakdown and provide justification if these expenses are necessary for project success. Click here to access the full expense report.

If these expenses are justified by specific project requirements or client needs, please document the business rationale for our records.

Thank you for your attention to this matter.

Best regards,
Priya Patel
Senior Partner
Finance & Admin Control Center
Perry Homes"""
                })
    except Exception as e:
        print(f"Error in generate_email: {e}")
        return jsonify({
            'error': 'Failed to generate email',
            'subject': 'Action Required',
            'body': 'An error occurred while generating the email. Please try again.'
        }), 500

@app.route('/hr-deepdive')
def hr_deepdive():
    """HR Deep-Dive page (placeholder)"""
    return render_template('coming_soon.html',
                          page_title='HR Deep-Dive',
                          description='People analytics and resource management')

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 8000))
    app.run(debug=False, host='0.0.0.0', port=port)