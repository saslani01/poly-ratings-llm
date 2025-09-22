function formatResponse(rawResponse) {
    const sections = rawResponse.split('\n\n');
    let formattedHTML = '';
    
    sections.forEach((section, index) => {
        section = section.trim();
        if (!section) return;
        
        if (index === 0 && section.includes('Professor ')) {
            const lines = section.split('\n');
            formattedHTML += `<h3>${lines[0]}</h3>`;
            return;
        }
        
        if (section.includes('Basic Stats')) {
            formattedHTML += '<div class="stats-section">';
            formattedHTML += '<h4>📊 Basic Stats (out of 4.0)</h4>';
            
            const lines = section.split('\n').slice(1); 
            lines.forEach(line => {
                if (line.includes('•') && line.includes(':')) {
                    const parts = line.replace('•', '').split(':');
                    if (parts.length === 2) {
                        const label = parts[0].trim();
                        let value = parts[1].trim();
                        
                        // Convert "/4" to "/4.0" for rating fields
                        if ((label.includes('Rating') || label.includes('Clarity') || label.includes('Difficulties')) && value.endsWith('/4')) {
                            value = value.replace('/4', '/4.0');
                        }
                        
                        formattedHTML += `
                            <div class="rating-item">
                                <span class="rating-label">${label}</span>
                                <span class="rating-value">${value}</span>
                            </div>
                        `;
                    }
                }
            });
            formattedHTML += '</div>';
            return;
        }
        
        if (section.includes('Review Excerpts Used:')) {
            formattedHTML += '<div class="excerpts-section">';
            formattedHTML += '<h4>📝 Review Excerpts Used</h4>';
            
            const lines = section.split('\n').slice(1); 
            lines.forEach(line => {
                if (line.includes('•') && line.includes('[') && line.includes(']')) {
                    const match = line.match(/•\s*\[([^\]]+)\]\s*(.+)/);
                    if (match) {
                        const aspect = match[1];
                        const content = match[2];
                        formattedHTML += `
                            <div class="excerpt-item">
                                <span class="excerpt-tag">${aspect}</span>
                                <div class="excerpt-text">"${content}"</div>
                            </div>
                        `;
                    }
                }
            });
            formattedHTML += '</div>';
            return;
        }
        
        if (!section.includes('Basic Stats') && !section.includes('Review Excerpts') && !section.includes('Professor ')) {
            formattedHTML += `
                <div class="ai-response">
                    <h4>🤖 AI Analysis</h4>
                    <div>${section.replace(/\n/g, '<br>')}</div>
                </div>
            `;
        }
    });
    
    return formattedHTML;
}

document.getElementById('queryForm').onsubmit = async function(e) {
    e.preventDefault();
    const query = document.getElementById('query').value;
    const resultDiv = document.getElementById('result');
    
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = '🤔 Analyzing professor reviews...';
    resultDiv.className = 'result loading';
    
    try {
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({query: query})
        });
        
        const data = await response.json();
        
        if (response.ok) {
            resultDiv.innerHTML = formatResponse(data.response);
            resultDiv.className = 'result';
        } else {
            if (response.status === 429) {
                resultDiv.innerHTML = '⚡ Rate limit reached! You can make 10 searches per minute. Please wait a moment and try again.';
            } else {
                resultDiv.innerHTML = 'Error: ' + (data.detail || 'Unknown error');
            }
            resultDiv.className = 'result error';
        }
    } catch (error) {
        resultDiv.innerHTML = 'Error: Failed to connect to server';
        resultDiv.className = 'result error';
    }
};