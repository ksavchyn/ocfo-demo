/**
 * Unified Modal JavaScript for CFO Dashboard
 * COMPLETE WORKING VERSION from Executive page
 * This file contains ALL modal-related functionality shared across all pages
 */

// Store last query results for export
window.lastQueryData = null;
window.lastQuerySQL = null;
window.lastQueryDescription = null;

// Store current request controller to cancel if needed
let currentRequestController = null;
let isRequestInProgress = false;
// Monotonic request ID — each askGenie() call bumps this. Every stream
// callback closure captures its own ID and bails if it no longer matches the
// current one. Prevents an in-flight response from a previous question from
// appending its tokens/sub-queries/follow-up chips into the chat for a NEW
// question (AbortController alone doesn't stop already-buffered SSE chunks).
let currentRequestId = 0;

// Auto-scroll only when the user is already pinned to the bottom.
// Threshold is generous because a single line of streamed text can push the
// distance up to ~24px even when the user hasn't moved.
function scrollChatToBottomIfPinned(el) {
    if (!el) return;
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    if (distanceFromBottom < 40) {
        el.scrollTop = el.scrollHeight;
    }
}

// Page-scoped demo questions are now sourced from gold_persona_insights via the
// /api/get-page-chips endpoint — orchestrator-generated, refreshed each cycle.
// In-memory cache keyed by page name; populated on first open per page per session.
const pageQuestionsCache = {};

async function fetchPageChips(page) {
    if (pageQuestionsCache[page]) {
        return pageQuestionsCache[page];
    }
    try {
        const resp = await fetch(`/api/get-page-chips?page=${encodeURIComponent(page)}`);
        if (!resp.ok) {
            console.warn('[MODAL] page chips fetch failed:', resp.status);
            return [];
        }
        const data = await resp.json();
        const chips = (data.chips || []).map(c => c.question_text).filter(Boolean);
        pageQuestionsCache[page] = chips;
        return chips;
    } catch (e) {
        console.warn('[MODAL] page chips fetch error:', e);
        return [];
    }
}

// Detect current page
function getCurrentPage() {
    // Check the URL path to determine which page we're on
    const path = window.location.pathname;
    console.log('[MODAL DEBUG] getCurrentPage - pathname:', path);
    console.log('[MODAL DEBUG] getCurrentPage - full URL:', window.location.href);

    if (path.includes('finance-deepdive')) {
        console.log('[MODAL DEBUG] Detected FINANCE page');
        return 'finance';
    }
    if (path.includes('admin-deepdive')) {
        console.log('[MODAL DEBUG] Detected ADMIN page');
        return 'admin';
    }
    // Default to executive for landing page
    console.log('[MODAL DEBUG] Defaulting to EXECUTIVE page');
    return 'executive';
}

// Open AI Assistant modal
window.openAIAssistant = function() {
    console.log('openAIAssistant called');

    // Check if modal elements exist
    const modal = document.getElementById('chatModal');
    if (!modal) {
        console.error('chatModal element not found!');
        return;
    }

    const chatMessages = document.getElementById('chatMessages');
    if (!chatMessages) {
        console.error('chatMessages element not found!');
        return;
    }

    // Clear any existing chat messages
    chatMessages.innerHTML = '';

    // Show the question selector view
    const questionView = document.getElementById('questionSelectorView');
    const chatView = document.getElementById('chatView');

    if (questionView) questionView.style.display = 'block';
    if (chatView) chatView.style.display = 'none';

    // Populate questions based on current PAGE (not persona!)
    const currentPage = getCurrentPage();
    console.log('Current page detected:', currentPage);
    console.log('Current URL:', window.location.pathname);
    populateModalQuestions(currentPage);

    // Open the chat modal
    modal.classList.add('show');

    // Clear the custom input
    const customInput = document.getElementById('modalCustomInput');
    if (customInput) customInput.value = '';
}

// Populate questions in the modal based on current page (async — fetches from
// gold_persona_insights via /api/get-page-chips). Renders a loading state first,
// then swaps in real chips when the fetch completes.
async function populateModalQuestions(page) {
    const questionsGrid = document.getElementById('modalQuestionsGrid');
    if (!questionsGrid) {
        console.error('Could not find modalQuestionsGrid element');
        return;
    }

    questionsGrid.innerHTML = `
        <div style="grid-column: 1 / -1; padding: 16px; color: #666; font-style: italic; text-align: center;">
            ⏳ Loading suggested questions...
        </div>
    `;

    const questions = await fetchPageChips(page);
    console.log('[MODAL DEBUG] page=', page, 'chips=', questions.length);

    if (!questions.length) {
        questionsGrid.innerHTML = `
            <div style="grid-column: 1 / -1; padding: 16px; color: #666;">
                No suggested questions available yet — type a question below to ask Genie directly.
            </div>
        `;
        return;
    }

    // Use data-question attribute pattern so apostrophes / quotes don't break onclick parsing.
    questionsGrid.innerHTML = questions.map(q => {
        const escaped = q
            .replace(/&/g, '&amp;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;');
        return `<button class="question-button" data-question="${escaped}" onclick="processQuestion(this.dataset.question)">${escaped}</button>`;
    }).join('');
}

// Process question from AI Assistant selector
window.processQuestion = function(question) {
    // Guard against double-fires while a previous question is still streaming.
    // Without this, a fast-fingered user (or a duplicate click on the same
    // follow-up chip) can stack multiple "Analyzing your question..." cards
    // in the chat. The backend AbortController DOES cancel the older request,
    // but the UI doesn't clean the old "analyzing" card before the new one
    // appears — which reads as a bug. Cheapest fix: drop the second click.
    if (isRequestInProgress) {
        console.log('[CHAT] processQuestion: another request in flight, ignoring click');
        return;
    }

    // Hide question selector, show chat view
    document.getElementById('questionSelectorView').style.display = 'none';
    const chatView = document.getElementById('chatView');
    chatView.style.display = 'flex';  // Use flex to maintain layout

    // Ensure chat input container is visible
    const chatInputContainer = document.querySelector('.chat-input-container');
    if (chatInputContainer) {
        chatInputContainer.style.display = 'flex';
    }

    // Set the question in the input and send it
    document.getElementById('chatInput').value = question;
    sendChatMessage();
}

// Click handler for the follow-up question chips rendered after a chat
// response. Disables ALL follow-up chips on the page immediately on click —
// gives instant visual feedback ("yes, I registered your click; please don't
// click again while it's working") AND prevents the double-fire scenario from
// the processQuestion guard above. Both layers are belt-and-suspenders for
// the same UX bug.
window.dispatchFollowupChip = function(button, question) {
    if (button && button.disabled) return;
    document.querySelectorAll('.suggested-question-chip').forEach(function(b) {
        b.disabled = true;
        b.style.opacity = '0.5';
        b.style.cursor = 'not-allowed';
    });
    window.processQuestion(question);
};

// Send custom question from modal
window.sendModalCustomQuestion = function() {
    const input = document.getElementById('modalCustomInput');
    const question = input.value.trim();

    if (question) {
        processQuestion(question);
    }
}

// Handle Enter key in modal input
window.handleModalInputKeyPress = function(event) {
    if (event.key === 'Enter') {
        sendModalCustomQuestion();
    }
}

// Send chat message
function sendChatMessage() {
    const input = document.getElementById('chatInput');
    const message = input.value.trim();

    if (!message) return;

    input.value = '';

    // Pass true for isFollowUp to preserve conversation history
    askGenie(message, true);
}

// Close chat modal
window.closeChatModal = function() {
    const modal = document.getElementById('chatModal');
    const modalContent = modal.querySelector('.chat-modal-content');

    modal.classList.add('closing');
    modal.classList.remove('show');

    // Reset view state for next opening
    document.getElementById('questionSelectorView').style.display = 'none';
    document.getElementById('chatView').style.display = 'block';

    if (modalContent) {
        modalContent.classList.add('fold-down');
    }

    setTimeout(() => {
        modal.style.display = 'none';
        modal.classList.remove('closing');
        if (modalContent) {
            modalContent.classList.remove('fold-down');
        }
    }, 300);
}

// Open chat modal
function openChatModal() {
    const modal = document.getElementById('chatModal');
    const modalContent = modal.querySelector('.chat-modal-content');

    // Clear the follow-up input as well to ensure fresh state
    const followUpInput = document.getElementById('followUpInput');
    if (followUpInput) {
        followUpInput.value = '';
    }

    modal.classList.remove('closing');
    modal.style.display = 'flex';

    // Trigger fold-up animation
    requestAnimationFrame(() => {
        modal.classList.add('show');
        if (modalContent) {
            modalContent.classList.add('fold-up');
        }
    });

    // Clean up animation class after animation completes
    setTimeout(() => {
        if (modalContent) {
            modalContent.classList.remove('fold-up');
        }
    }, 600);
}

// Main askGenie function - COMPLETE VERSION WITH ALL STREAMING FUNCTIONALITY
function askGenie(question, isFollowUp = false) {
    const messages = document.getElementById('chatMessages');

    // CANCEL ANY IN-FLIGHT REQUEST BEFORE TOUCHING THE DOM. If the user clicks
    // a new question while a previous answer is still streaming, the old SSE
    // reader can still emit buffered chunks for a few hundred ms after abort.
    // Bump the request ID first so any straggler callbacks bail out (see the
    // myRequestId guard below).
    const myRequestId = ++currentRequestId;
    if (currentRequestController) {
        currentRequestController.abort();
    }

    // Always clear previous messages for preset questions (when not a follow-up)
    // This ensures each preset question click starts fresh
    if (!isFollowUp) {
        messages.innerHTML = '';
        // Reset any stored data as well
        window.lastQueryData = null;
        window.lastQuerySQL = null;
        window.lastQueryDescription = null;
        // Reset conversation history when starting a new top-level question.
        // Follow-up chips (isFollowUp=true) preserve the running history so the
        // backend can disambiguate context (Chicago / expense category / month).
        window.chatHistory = [];
        openChatModal();
    }
    if (!window.chatHistory) window.chatHistory = [];

    // Add user's question
    const userDiv = document.createElement('div');
    userDiv.className = 'chat-message user';
    userDiv.innerHTML = `<strong>You:</strong> ${question}`;
    messages.appendChild(userDiv);

    // Add "analyzing" status with steps container
    const statusDiv = document.createElement('div');
    statusDiv.className = 'chat-message thought-process';
    statusDiv.innerHTML = `
        <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 8px;">
            <div class="spinner" style="width: 16px; height: 16px; border: 2px solid #007acc; border-top-color: transparent; border-radius: 50%; animation: spin 1s linear infinite;"></div>
            <em style="color: #007acc;">Analyzing your question...</em>
        </div>
        <div class="genie-steps" style="margin-top: 8px; font-size: 12px; color: #4a5568;"></div>
    `;
    messages.appendChild(statusDiv);
    scrollChatToBottomIfPinned(messages);

    // Create new AbortController for this request
    currentRequestController = new AbortController();
    isRequestInProgress = true;

    // Tracks the most-recent in-progress status step so we can flip its dots
    // to ✓ when the next phase begins. Reset per question.
    let currentInProgressStatus = null;
    function markInProgressStatusComplete() {
        if (currentInProgressStatus) {
            const indicator = currentInProgressStatus.querySelector('.status-indicator');
            if (indicator) {
                indicator.innerHTML = '✓';
                indicator.style.color = '#28a745';
            }
            currentInProgressStatus = null;
        }
    }

    // Get current persona for API (still needed for backend)
    const persona = sessionStorage.getItem('selectedPersona') || 'priya';
    let personaType = persona;
    if (persona === 'sarah') personaType = 'finance';
    else if (persona === 'priya') personaType = 'admin';
    else if (persona === 'michael') personaType = 'hr';

    let messageBuffer = '';
    let responseStarted = false;
    let currentResponseDiv = null;
    let currentSQL = '';

    // Snapshot conversation history BEFORE pushing the current user question,
    // so the request body's `history` is everything prior (current question
    // travels separately in the `message` field). The assistant reply gets
    // pushed onto window.chatHistory once the stream finishes.
    const priorHistory = (window.chatHistory || []).slice();
    window.chatHistory.push({ role: 'user', content: question });

    // Use the CORRECT streaming endpoint
    fetch('/api/chat/stream', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            message: question,
            persona: personaType,
            history: priorHistory
        }),
        signal: currentRequestController.signal
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        function processStream() {
            reader.read().then(({ done, value }) => {
                // GUARD: bail if a newer askGenie() call has superseded this one.
                // The abort signal stops new chunks but already-buffered chunks
                // still arrive — without this guard, a stale stream's tokens or
                // suggested-question chips would get appended into the chat for
                // the new question. Cancel the reader so the stream tears down
                // and exit silently.
                if (myRequestId !== currentRequestId) {
                    try { reader.cancel(); } catch (_) { /* already torn down */ }
                    return;
                }
                if (done) {
                    // Final formatting when done
                    if (messageBuffer && !responseStarted) {
                        // Keep status visible with the steps
                        const spinnerDiv = statusDiv.querySelector('.spinner');
                        if (spinnerDiv) {
                            spinnerDiv.style.display = 'none';
                        }
                        const statusText = statusDiv.querySelector('em');
                        if (statusText) {
                            statusText.textContent = 'Analysis complete';
                            statusText.style.color = '#28a745';
                        }
                        // Create a new message for non-streamed response
                        addFormattedMessage('genie', messageBuffer);
                    }
                    // Persist the assistant's full reply into the rolling chat
                    // history so the next turn's request body carries it back to
                    // the backend. Cap at last 12 entries (~6 user/assistant pairs)
                    // to keep payloads small.
                    if (messageBuffer && window.chatHistory) {
                        window.chatHistory.push({ role: 'assistant', content: messageBuffer });
                        if (window.chatHistory.length > 12) {
                            window.chatHistory = window.chatHistory.slice(-12);
                        }
                    }
                    isRequestInProgress = false;
                    currentRequestController = null;
                    return;
                }

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop() || '';

                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        try {
                            const data = JSON.parse(line.substring(6));
                            console.log('Stream data received:', data.type, data);

                            if (data.type === 'status') {
                                // Mark the previous in-progress status step (if any) as complete first
                                markInProgressStatusComplete();
                                // Then render this new status with animated in-progress dots
                                const stepsDiv = statusDiv.querySelector('.genie-steps');
                                if (stepsDiv) {
                                    const stepItem = document.createElement('div');
                                    stepItem.style.cssText = 'margin: 4px 0; padding-left: 20px; position: relative;';
                                    stepItem.innerHTML = `
                                        <span class="status-indicator" style="position: absolute; left: 0; color: #6b7280;"><span class="status-loading-dots"><span>.</span><span>.</span><span>.</span></span></span>
                                        <span>${data.message}</span>
                                    `;
                                    stepsDiv.appendChild(stepItem);
                                    currentInProgressStatus = stepItem;
                                }
                            } else if (data.type === 'query_detail') {
                                // Show query details as sub-steps
                                const stepsDiv = statusDiv.querySelector('.genie-steps');
                                if (stepsDiv) {
                                    const detailItem = document.createElement('div');
                                    detailItem.style.cssText = 'margin: 2px 0 2px 40px; font-size: 11px; color: #6b7280;';
                                    detailItem.innerHTML = `• ${data.content}`;
                                    stepsDiv.appendChild(detailItem);
                                }
                            } else if (data.type === 'sql') {
                                // Store SQL but don't display here - it will be shown in tool_result
                                currentSQL = data.content;
                            } else if (data.type === 'tool_start') {
                                // A sub-query is starting — the previous status step is implicitly done.
                                // Render the SUB-QUESTION itself as the visual header (no "Querying X"
                                // wrapper — that read as repetitive boilerplate to users since every
                                // sub-query hit the same Genie space). The italicized question text
                                // is the natural divider between sub-queries; the icon (🔍) signals
                                // "query running"; on tool_result it morphs to ✓ for "query done".
                                // Identical behavior for both cached chip-click replays and live
                                // user-typed questions — they use the same SSE event stream.
                                markInProgressStatusComplete();
                                const stepsDiv = statusDiv.querySelector('.genie-steps');
                                if (stepsDiv) {
                                    const stepItem = document.createElement('div');
                                    stepItem.style.cssText = 'margin: 12px 0 4px 0; padding-left: 20px; position: relative;';
                                    const questionText = data.question ? `"${data.question}"` : `Sub-query ${(stepsDiv.querySelectorAll('[data-subquery-marker]').length || 0) + 1}`;
                                    stepItem.setAttribute('data-subquery-marker', '1');
                                    stepItem.innerHTML = `
                                        <span style="position: absolute; left: 0;" class="subquery-icon">🔍</span>
                                        <span style="color: #374151; font-style: italic; font-size: 13px;" class="subquery-question">${questionText}</span>
                                    `;
                                    stepsDiv.appendChild(stepItem);
                                }
                            } else if (data.type === 'tool_result') {
                                // Show results with rich metadata in steps format
                                const stepsDiv = statusDiv.querySelector('.genie-steps');
                                if (stepsDiv) {
                                    // Debug logging for all tool results
                                    console.log('Tool result received:', {
                                        tool: data.tool,
                                        has_raw_data: !!data.raw_data,
                                        raw_data_length: data.raw_data ? data.raw_data.length : 0,
                                        raw_data_sample: data.raw_data ? data.raw_data[0] : null,
                                        description: data.description
                                    });

                                    // Store raw data if available
                                    if (data.raw_data && Array.isArray(data.raw_data) && data.raw_data.length > 0) {
                                        window.lastQueryData = data.raw_data;
                                        console.log('✅ Stored raw query data:', data.raw_data.length, 'rows');
                                        console.log('Sample row:', data.raw_data[0]);
                                    } else {
                                        console.log('⚠️ No raw_data in tool result');
                                    }

                                    // Morph the most-recent sub-query's icon from 🔍 (running) to ✓ (done).
                                    // Visual state transition so the user can see which sub-queries have
                                    // finished as they stream in.
                                    const subqueryMarkers = stepsDiv.querySelectorAll('[data-subquery-marker]');
                                    if (subqueryMarkers.length > 0) {
                                        const lastMarker = subqueryMarkers[subqueryMarkers.length - 1];
                                        const icon = lastMarker.querySelector('.subquery-icon');
                                        if (icon) {
                                            icon.innerHTML = '✓';
                                            icon.style.color = '#27ae60';
                                        }
                                    }

                                    // Show interpretation/description immediately after the query (paired)
                                    if (data.description) {
                                        window.lastQueryDescription = data.description;
                                        const descItem = document.createElement('div');
                                        descItem.style.cssText = 'margin: 4px 0 8px 40px; font-size: 11px; color: #6b7280; font-style: italic;';
                                        // Render **bold** markdown inline. The Genie sub-query summary text
                                        // ships with markdown asterisks (e.g. "**billable revenue**") that
                                        // were previously displayed as raw `**` characters.
                                        const rendered = String(data.description)
                                            .replace(/&/g, '&amp;')
                                            .replace(/</g, '&lt;')
                                            .replace(/>/g, '&gt;')
                                            .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
                                        descItem.innerHTML = rendered;
                                        stepsDiv.appendChild(descItem);
                                    }

                                    if (data.sql) {
                                        window.lastQuerySQL = data.sql;
                                        const formattedSQL = formatSQL(data.sql);
                                        const sqlContainer = document.createElement('div');
                                        sqlContainer.style.cssText = 'margin: 8px 0 8px 20px;';
                                        sqlContainer.innerHTML = `
                                            <details style="margin: 0;">
                                                <summary style="cursor: pointer; color: #3498db; font-size: 12px;">View SQL Query</summary>
                                                <pre class="sql-query-display" style="background: #fafafa; color: #374151; padding: 12px; border-radius: 6px; margin-top: 8px; font-size: 12px; font-family: 'Monaco', 'Consolas', monospace; overflow-x: auto; border: 1px solid #34495e; line-height: 1.4; max-height: 300px; overflow-y: auto;">${escapeHtml(formattedSQL)}</pre>
                                            </details>
                                        `;
                                        stepsDiv.appendChild(sqlContainer);
                                    }

                                    if (data.tables && data.tables.length > 0) {
                                        const tablesItem = document.createElement('div');
                                        tablesItem.style.cssText = 'margin: 4px 0 4px 40px; font-size: 11px; color: #6b7280;';
                                        tablesItem.innerHTML = `📊 Tables accessed: ${data.tables.join(', ')}`;
                                        stepsDiv.appendChild(tablesItem);
                                    }

                                    if (data.row_count !== null && data.row_count !== undefined) {
                                        const rowsItem = document.createElement('div');
                                        rowsItem.style.cssText = 'margin: 4px 0 4px 40px; font-size: 11px; color: #6b7280;';
                                        rowsItem.innerHTML = `📈 ${data.row_count} rows found`;
                                        stepsDiv.appendChild(rowsItem);

                                        // Always show export button when we have any rows
                                        if (data.row_count !== null && data.row_count !== undefined) {
                                            const exportBtn = document.getElementById('exportDataBtn');
                                            if (exportBtn) {
                                                exportBtn.style.display = 'block';
                                                // Always update metadata for export with the latest info
                                                window.exportMetadata = {
                                                    rowCount: data.row_count,
                                                    sql: window.lastQuerySQL || currentSQL,
                                                    description: window.lastQueryDescription,
                                                    tables: data.tables || [],
                                                    timestamp: new Date().toISOString()
                                                };
                                            }
                                        }
                                    }
                                }
                            } else if (data.type === 'clear_status') {
                                // Synthesis is fully done — finalize any in-progress status step
                                markInProgressStatusComplete();
                                // Don't hide status - keep it visible to show steps
                            } else if (data.type === 'token') {
                                if (!responseStarted) {
                                    responseStarted = true;
                                    // First token = synthesis is producing output, prior status is done
                                    markInProgressStatusComplete();
                                    // Don't hide status - keep the steps visible
                                    // Update the status to show it's complete
                                    const spinnerDiv = statusDiv.querySelector('.spinner');
                                    if (spinnerDiv) {
                                        spinnerDiv.style.display = 'none';
                                    }
                                    const statusText = statusDiv.querySelector('em');
                                    if (statusText) {
                                        statusText.textContent = 'Analysis complete';
                                        statusText.style.color = '#28a745';
                                    }
                                    // Create a new response div for this specific response
                                    currentResponseDiv = document.createElement('div');
                                    currentResponseDiv.className = 'chat-message genie response';
                                    messages.appendChild(currentResponseDiv);
                                }
                                messageBuffer += data.content;
                                // Update the specific response div for this request
                                if (currentResponseDiv) {
                                    const formatted = formatMarkdown(messageBuffer);
                                    currentResponseDiv.innerHTML = `<strong>Genie:</strong> <div style="font-size: 13px; line-height: 1.5;">${formatted}</div>`;
                                    scrollChatToBottomIfPinned(messages);
                                }
                                // Parse for table data if present
                                if (messageBuffer.includes('|') && messageBuffer.includes('---')) {
                                    // Don't overwrite lastQueryData here - it should only be set from raw_data
                                    // Only show export button if we have actual exportable data
                                    const exportBtn = document.getElementById('exportDataBtn');
                                    if (exportBtn && window.lastQueryData && window.lastQueryData.length > 0) {
                                        exportBtn.style.display = 'block';
                                    }
                                }
                            } else if (data.type === 'suggested_questions') {
                                // Render Genie/Claude-suggested follow-up question chips
                                const questions = data.questions || [];
                                if (questions.length > 0 && currentResponseDiv) {
                                    const chipsContainer = document.createElement('div');
                                    chipsContainer.className = 'suggested-questions-chips';
                                    chipsContainer.style.cssText = 'margin-top: 12px; display: flex; flex-wrap: wrap; gap: 8px;';
                                    chipsContainer.innerHTML = `
                                        <div style="width: 100%; font-size: 11px; color: #666; margin-bottom: 4px;">Suggested follow-ups:</div>
                                        ${questions.map(q => `
                                            <button class="suggested-question-chip"
                                                onclick="dispatchFollowupChip(this, '${q.replace(/'/g, "\\'")}')"
                                                style="background: #f0f7ff; border: 1px solid #007acc; color: #007acc; padding: 6px 12px; border-radius: 16px; font-size: 12px; cursor: pointer; text-align: left; max-width: 100%;">
                                                ${q}
                                            </button>
                                        `).join('')}
                                    `;
                                    currentResponseDiv.appendChild(chipsContainer);
                                    scrollChatToBottomIfPinned(messages);
                                }
                            } else if (data.type === 'error') {
                                // Error short-circuits any in-progress status step
                                markInProgressStatusComplete();
                                // Keep status visible but update to show error
                                const spinnerDiv = statusDiv.querySelector('.spinner');
                                if (spinnerDiv) {
                                    spinnerDiv.style.display = 'none';
                                }
                                const statusText = statusDiv.querySelector('em');
                                if (statusText) {
                                    statusText.textContent = 'Error occurred';
                                    statusText.style.color = '#dc3545';
                                }
                                addFormattedMessage('genie', `Error: ${data.message}`);
                            }
                        } catch (e) {
                            console.error('Error parsing SSE data:', e);
                        }
                    }
                }
                processStream();
            }).catch(err => {
                // If this stream belongs to a superseded request, swallow silently
                // — the new request owns the chat now.
                if (myRequestId !== currentRequestId) {
                    return;
                }
                if (err.name === 'AbortError') {
                    console.log('Request was cancelled');
                } else {
                    console.error('Stream reading error:', err);
                    // Try fallback to regular chat endpoint
                    fetch('/api/chat', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            message: question,
                            persona: personaType
                        })
                    })
                    .then(response => response.json())
                    .then(data => {
                        if (myRequestId !== currentRequestId) return;
                        // Hide spinner
                        const spinnerDiv = statusDiv.querySelector('.spinner');
                        if (spinnerDiv) {
                            spinnerDiv.style.display = 'none';
                        }
                        const statusText = statusDiv.querySelector('em');
                        if (statusText) {
                            statusText.textContent = 'Analysis complete';
                            statusText.style.color = '#28a745';
                        }
                        // Show response
                        const reply = data.reply || data.message || 'No response received';
                        addFormattedMessage('genie', reply);
                    })
                    .catch(error => {
                        if (myRequestId !== currentRequestId) return;
                        console.error('Fallback error:', error);
                        addFormattedMessage('genie', 'Sorry, I encountered an error processing your request.');
                    });
                }
                isRequestInProgress = false;
                currentRequestController = null;
            });
        }

        processStream();
    })
    .catch(err => {
        // Stale stream — don't clobber the new request's state and don't
        // append a fallback answer to the new chat.
        if (myRequestId !== currentRequestId) {
            return;
        }
        console.error('Error:', err);
        if (err.name !== 'AbortError') {
            // Try the non-streaming endpoint as fallback
            fetch('/api/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    message: question,
                    persona: personaType
                })
            })
            .then(response => response.json())
            .then(data => {
                if (myRequestId !== currentRequestId) return;
                // Hide spinner
                const spinnerDiv = statusDiv.querySelector('.spinner');
                if (spinnerDiv) {
                    spinnerDiv.style.display = 'none';
                }
                const statusText = statusDiv.querySelector('em');
                if (statusText) {
                    statusText.textContent = 'Analysis complete';
                    statusText.style.color = '#28a745';
                }
                // Show response
                const reply = data.reply || data.message || 'No response received';
                addFormattedMessage('genie', reply);
            })
            .catch(error => {
                if (myRequestId !== currentRequestId) return;
                console.error('Fallback error:', error);
                addFormattedMessage('genie', 'Sorry, I encountered an error processing your request.');
            });
        }
        isRequestInProgress = false;
        currentRequestController = null;
    });
}

// Format SQL for display
function formatSQL(sql) {
    if (!sql) return '';

    // Basic SQL formatting - add newlines and indentation
    let formatted = sql
        .replace(/\bSELECT\b/gi, 'SELECT')
        .replace(/\bFROM\b/gi, '\nFROM')
        .replace(/\bWHERE\b/gi, '\nWHERE')
        .replace(/\bGROUP BY\b/gi, '\nGROUP BY')
        .replace(/\bORDER BY\b/gi, '\nORDER BY')
        .replace(/\bLIMIT\b/gi, '\nLIMIT')
        .replace(/\bJOIN\b/gi, '\n  JOIN')
        .replace(/\bLEFT JOIN\b/gi, '\n  LEFT JOIN')
        .replace(/\bRIGHT JOIN\b/gi, '\n  RIGHT JOIN')
        .replace(/\bINNER JOIN\b/gi, '\n  INNER JOIN')
        .replace(/\bAND\b/gi, '\n  AND')
        .replace(/\bOR\b/gi, '\n  OR')
        .replace(/,\s+/g, ',\n       ');

    return formatted;
}

// Escape HTML for safe display
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Add formatted message
function addFormattedMessage(sender, content) {
    const messages = document.getElementById('chatMessages');
    const messageDiv = document.createElement('div');
    messageDiv.className = `chat-message ${sender} response`;

    if (sender === 'genie') {
        const formatted = formatMarkdown(content);
        // Increased font size from 11px to 13px for better readability
        messageDiv.innerHTML = `<strong>Genie:</strong> <div style="font-size: 13px; line-height: 1.5;">${formatted}</div>`;
    } else {
        messageDiv.innerHTML = `<strong>You:</strong> ${content}`;
    }

    messages.appendChild(messageDiv);
    scrollChatToBottomIfPinned(messages);
}

// Format markdown content
function formatMarkdown(text) {
    // Use marked.js for proper markdown parsing if available
    if (typeof marked !== 'undefined') {
        // Configure marked for consistent output
        marked.setOptions({
            breaks: true,
            gfm: true,  // Keep GFM enabled for tables
            headerIds: false,
            mangle: false
        });

        // Disable strikethrough entirely — the agent prompt forbids it, so any ~~
        // in the response is unintended (e.g., model-emitted tildes near digits).
        text = text.replace(/~~/g, '\\~\\~');

        // Parse markdown and apply custom CSS classes
        let html = marked.parse(text);

        // Apply consistent styling to headers
        html = html.replace(/<h1>/g, '<h1 style="font-size: 16px; font-weight: 600; margin: 12px 0 8px 0;">');
        html = html.replace(/<h2>/g, '<h2 style="font-size: 15px; font-weight: 600; margin: 12px 0 8px 0;">');
        html = html.replace(/<h3>/g, '<h3 style="font-size: 14px; font-weight: 600; margin: 10px 0 6px 0;">');
        html = html.replace(/<h4>/g, '<h4 style="font-size: 13px; font-weight: 500; margin: 8px 0 6px 0;">');
        html = html.replace(/<h5>/g, '<h5 style="font-size: 13px; font-weight: 500; margin: 8px 0 6px 0;">');
        html = html.replace(/<h6>/g, '<h6 style="font-size: 13px; font-weight: 500; margin: 8px 0 6px 0;">');

        // Style paragraphs
        html = html.replace(/<p>/g, '<p style="font-size: 13px; line-height: 1.5; margin: 8px 0;">');

        // Style lists
        html = html.replace(/<ul>/g, '<ul style="margin: 8px 0; padding-left: 20px;">');
        html = html.replace(/<ol>/g, '<ol style="margin: 8px 0; padding-left: 20px;">');
        html = html.replace(/<li>/g, '<li style="font-size: 13px; line-height: 1.5; margin: 4px 0;">');

        // Style code blocks
        html = html.replace(/<pre>/g, '<pre style="background: #1e2329; padding: 10px; border-radius: 4px; margin: 8px 0; overflow-x: auto;">');
        html = html.replace(/<code>/g, '<code style="font-size: 12px; font-family: monospace;">');

        // Style tables
        html = html.replace(/<table>/g, '<table style="border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px;">');
        html = html.replace(/<thead>/g, '<thead style="background: #f3f4f6; border-bottom: 2px solid #d1d5db;">');
        html = html.replace(/<th>/g, '<th style="padding: 8px 12px; text-align: left; font-weight: 600;">');
        html = html.replace(/<td>/g, '<td style="padding: 8px 12px; border-bottom: 1px solid #e5e7eb;">');
        html = html.replace(/<tr>/g, '<tr style="border-bottom: 1px solid #e5e7eb;">');

        return html;
    }

    // Fallback manual parsing if marked.js is not available
    let formatted = text;

    // Convert SQL code blocks with readable font
    formatted = formatted.replace(/```sql\n([\s\S]+?)```/g,
        '<pre style="background: #f0f0f0; padding: 8px; border-radius: 4px; margin: 8px 0; overflow-x: auto; font-size: 12px; font-family: monospace;">$1</pre>');

    // Convert other code blocks with readable font
    formatted = formatted.replace(/```\n([\s\S]+?)```/g,
        '<pre style="background: #f0f0f0; padding: 8px; border-radius: 4px; margin: 8px 0; overflow-x: auto; font-size: 12px; font-family: monospace;">$1</pre>');

    // Add line breaks (but not after headers or divs)
    formatted = formatted.replace(/\n\n(?![<\/])/g, '<br><br>');
    formatted = formatted.replace(/\n(?![<\/])/g, '<br>');

    return formatted;
}

// Export data to CSV - Clean data only, no metadata
window.exportData = function() {
    if (!window.lastQueryData || window.lastQueryData.length === 0) {
        alert('No data available to export');
        return;
    }

    try {
        const data = window.lastQueryData;
        const headers = Object.keys(data[0]);

        // Create CSV content - properly quoted for Excel/Sheets compatibility
        let csvContent = headers.map(h => `"${h}"`).join(',') + '\n';

        data.forEach(row => {
            const values = headers.map(header => {
                const value = row[header];
                if (value === null || value === undefined) return '""';
                const str = String(value);
                // Escape quotes and wrap in quotes
                const escaped = str.replace(/"/g, '""');
                return `"${escaped}"`;
            });
            csvContent += values.join(',') + '\n';
        });

        // Just use the clean CSV content - NO metadata
        const fullContent = csvContent;

        // Create download link
        const blob = new Blob([fullContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);

        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5);
        link.setAttribute('href', url);
        link.setAttribute('download', `query_results_${timestamp}.csv`);
        link.style.visibility = 'hidden';

        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    } catch (error) {
        console.error('Export error:', error);
        alert('Error exporting data. Please try again.');
    }
}

// Initialize event listeners when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    // Allow Enter key to send message
    const chatInput = document.getElementById('chatInput');
    if (chatInput) {
        chatInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                sendChatMessage();
            }
        });
    }

    // Close modal when clicking outside
    const chatModal = document.getElementById('chatModal');
    if (chatModal) {
        chatModal.addEventListener('click', function(e) {
            if (e.target === this) {
                closeChatModal();
            }
        });
    }
});