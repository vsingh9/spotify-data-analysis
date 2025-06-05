function fetchFeatureImportance() {
    fetch('/get_feature_importance')
        .then(response => {
            if (!response.ok) {
                throw new Error("Failed to fetch feature importance data.");
            }
            return response.json();
        })
        .then(data => {
            const container = document.getElementById("feature-importance-output");
            container.innerHTML = "";

            if (data.length === 0) {
                container.textContent = "No data available.";
                return;
            }

            const table = document.createElement("table");
            table.border = "1";
            table.style.borderCollapse = "collapse";
            table.style.width = "80%";

            const header = table.insertRow();
            header.insertCell().textContent = "Feature";
            header.insertCell().textContent = "Correlation Coefficient";

            data.forEach(row => {
                const tr = table.insertRow();
                tr.insertCell().textContent = row.feature;
                tr.insertCell().textContent = row.coefficient.toFixed(3);
            });

            container.appendChild(table);
        })
        .catch(error => {
            console.error(error);
            document.getElementById("feature-importance-output").textContent = "Error loading data.";
        });
}

function clearFeatureImportance() {
    document.getElementById("feature-importance-output").innerHTML = "";
}
