function formatResponse(data) {
    let html = '';

    html += `
        <div class="card shadow-sm mb-3">
            <div class="card-body">
                <h3 class="card-title text-success border-bottom pb-2">
                    Professor ${data.professor.name} - ${data.professor.department}
                </h3>
    `;

    const stats = [
        ['Overall Rating', `${data.stats.overall_rating}/4.0`],
        ['Material Clarity', `${data.stats.material_clear}/4.0`],
        ['Recognizing Student Difficulties', `${data.stats.student_difficulties}/4.0`],
        ['Number of Evaluations', data.stats.num_evals],
    ];

    html += `
        <div class="bg-light rounded p-3 mb-3">
            <h5 class="text-success">Basic Stats (out of 4.0)</h5>
            <ul class="list-group list-group-flush">
    `;
    stats.forEach(([label, value]) => {
        html += `
            <li class="list-group-item d-flex justify-content-between bg-transparent px-0">
                <span>${label}</span>
                <span class="fw-bold text-success">${value}</span>
            </li>
        `;
    });
    html += `</ul></div>`;

    html += `
        <div class="border-start border-4 border-warning bg-light rounded p-3 mb-3">
            <h5 class="text-success">AI Analysis</h5>
            <div>${data.analysis}</div>
        </div>
    `;

    if (data.excerpts && data.excerpts.length > 0) {
        html += `<h5 class="text-success"> Review Excerpts Used</h5>`;
        data.excerpts.forEach(excerpt => {
            html += `
                <div class="card mb-2 border-start border-3 border-success">
                    <div class="card-body py-2">
                        <span class="badge bg-success text-uppercase mb-2">${excerpt.aspect}</span>
                        <p class="fst-italic text-secondary mb-0">"${excerpt.content}"</p>
                    </div>
                </div>
            `;
        });
    }

    html += `</div></div>`;
    return html;
}

function submitHandler(e) {
    e.preventDefault();
    const query = document.getElementById('query').value;
    const resultDiv = document.getElementById('result');

    resultDiv.innerHTML = `
        <div class="text-center text-muted py-4">
            <div class="spinner-border text-success"></div>
            <p class="mt-2 mb-0"> Analyzing professor reviews...</p>
        </div>
    `;

    fetch('/api/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query })
    }).then((res) => {
        if (res.status === 200) {
            return res.json();
        }

        else if (res.status === 404) {
            throw new Error("Professor not found");
        }

        else if (res.status === 422) {
            throw new Error("Invalid Input");
        }

        else if (res.status === 429) {
            throw new Error("Rate Limit Exceeded");
        }

        else if (res.status === 500) {
            throw new Error("nternal Server Error");
        }

        else if (res.status === 503) {
            throw new Error("Service busy. Please try again shortly.");
        }

        else {
            throw new Error("Unexpected error");
        }


    }).then((data) => {
        resultDiv.innerHTML = formatResponse(data.response);
    }).catch((err) => {
        resultDiv.innerHTML = `<div class="alert alert-danger">${err.message}</div>`;
    })
}

document.getElementById('queryForm').onsubmit = submitHandler;