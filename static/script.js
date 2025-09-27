function formatResponse(data) {
    if (data.error) {
        return `<div class="error">${data.error}</div>`;
    }
    
    let formattedHTML = '';
    
    formattedHTML += `<h3>Professor ${data.professor.name} - ${data.professor.department}</h3>`;
    
    formattedHTML += '<div class="stats-section">';
    formattedHTML += '<h4>📊 Basic Stats (out of 4.0)</h4>';
    formattedHTML += `
        <div class="rating-item">
            <span class="rating-label">Overall Rating</span>
            <span class="rating-value">${data.stats.overall_rating}/4.0</span>
        </div>
        <div class="rating-item">
            <span class="rating-label">Material Clarity</span>
            <span class="rating-value">${data.stats.material_clear}/4.0</span>
        </div>
        <div class="rating-item">
            <span class="rating-label">Recognizing Student Difficulties</span>
            <span class="rating-value">${data.stats.student_difficulties}/4.0</span>
        </div>
        <div class="rating-item">
            <span class="rating-label">Number of Evaluations</span>
            <span class="rating-value">${data.stats.num_evals}</span>
        </div>
    `;
    formattedHTML += '</div>';
    
    formattedHTML += `
        <div class="ai-response">
            <h4>🤖 AI Analysis</h4>
            <div>${data.analysis}</div>
        </div>
    `;
    
    if (data.excerpts && data.excerpts.length > 0) {
        formattedHTML += '<div class="excerpts-section">';
        formattedHTML += '<h4>📝 Review Excerpts Used</h4>';
        
        data.excerpts.forEach(excerpt => {
            formattedHTML += `
                <div class="excerpt-item">
                    <span class="excerpt-tag">${excerpt.aspect}</span>
                    <div class="excerpt-text">"${excerpt.content}"</div>
                </div>
            `;
        });
        formattedHTML += '</div>';
    }
    
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