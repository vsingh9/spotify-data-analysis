document.addEventListener("DOMContentLoaded", () => {
    fetch("/get_explicit_genres")
        .then(res => res.json())
        .then(genres => {
            const select = document.getElementById("genreSelect");
            genres.forEach(genre => {
                const option = document.createElement("option");
                option.value = genre;
                option.textContent = genre;
                select.appendChild(option);
            });
        });
});

// Fetch popularity data when a genre is selected
function loadExplicitPopularity() {
    const genre = document.getElementById("genreSelect").value;
    if (!genre) return;

    fetch(`/get_explicit_popularity_by_genre/${encodeURIComponent(genre)}`)
        .then(res => res.json())
        .then(data => {
            const resultDiv = document.getElementById("explicitResults");
            resultDiv.innerHTML = `<h3>Popularity in ${genre}</h3>`;
            data.forEach(entry => {
                const label = entry.explicit ? "Explicit" : "Non-Explicit";
                resultDiv.innerHTML += `<p><strong>${label}</strong>: ${entry.popularity}</p>`;
            });
        });
}

