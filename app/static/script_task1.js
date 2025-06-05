document.addEventListener('DOMContentLoaded', () => {
    const genre_select = document.getElementById('genre-select')
    const emotion_select = document.getElementById('emotion-select')

    // Load genres
    fetch('/get_genres')
        .then(res => res.json())
        .then(genres => {
            genre_select.innerHTML = '<option value=""> -- Select Genre -- </option>';
            genres.forEach(g => {
                const option = document.createElement("option");
                option.value = g;
                option.textContent = g;
                genre_select.appendChild(option)
            })
        })
        .catch(() => {
            genre_select.innerHTML = "<option value=''>Failed to load genres</option>";
        })

    // Load emotions
    fetch('/get_emotions')
        .then(res => res.json())
        .then(emotions => {
            emotion_select.innerHTML = '<option value=""> -- Select Emotion -- </option>';
            emotions.forEach(e => {
                const option = document.createElement("option");
                option.value = e;
                option.textContent = e;
                emotion_select.appendChild(option)
            })
        })
        .catch(() => {
            emotion_select.innerHTML = "<option value=''>Failed to load emotions</option>";
        })
})

function filter_count() {
    const count = document.getElementById('count-input').value.trim();
    const output_div = document.getElementById('list-data-output');
    // clear prev results
    output_div.innerHTML = "";

    fetch(`/filter_count?min_count=${count}`)
        .then(res => res.json())
        .then(data => {
            data.forEach(d => {
                const p = document.createElement("p");
                p.textContent = `${d[0]}, ${d[1]}, Count: ${d[2]}`;
                output_div.appendChild(p)
            })
        })
        .catch(error => {
            console.error("Error fetching data:", error)
        })
}

function fetch_genre_emotion_count() {
    const genre = document.getElementById('genre-select').value.trim();
    const emotion = document.getElementById('emotion-select').value.trim();
    const output_div = document.getElementById('data-output');
    // clear prev results
    output_div.innerHTML = "";

    // const [genre, emotion] = input.split(',').map(x => x.trim());

    if (!genre || !emotion || emotion == 'True' || emotion == 'pink') {
        output_div.innerText = "Please enter both a valid genre and a valid emotion";
        return;
    }

    const url = `/get_genre_emotion_count?genre=${encodeURIComponent(genre)}&emotion=${encodeURIComponent(emotion)}`

    fetch(url)
        .then(res => res.json())
        .then(data => {
            const found = data.genre_emotion_count.find(
                entry => entry[0].toLowerCase() === genre.toLowerCase() &&
                         entry[1].toLowerCase() === emotion.toLowerCase()
            )

            if (found) {
                const count = found[2]
                output_div.innerText = `Genre: ${genre}, Emotion: ${emotion}, Count: ${count}`
            } else {
                output_div.innerText = "Combination does not exist."
            }
        })
        .catch(error => {
            console.error("Error fetching data:", error);
        });
}

