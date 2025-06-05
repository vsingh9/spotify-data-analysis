document.addEventListener('DOMContentLoaded', () => {
    const category_dropdown = document.getElementById('category-dropdown')
    const emotion_dropdown = document.getElementById('emotion-dropdown')

    //load categories
    fetch('/get_categories')
        .then(res => res.json())
        .then(categories => {
            category_dropdown.innerHTML = '<option value=""> -- Select Category -- </option>';
            categories.forEach(c => {
                const option = document.createElement("option");
                option.value = c;
                option.textContent = c;
                category_dropdown.appendChild(option)
            })
        })
        .catch(() => {
            category_dropdown.innerHTML = "<option value=''>Failed to load categories</option>";
        })

    //load emotions
    fetch('/get_emotions_4')
        .then(res => res.json())
        .then(emotions => {
            emotion_dropdown.innerHTML = '<option value=""> -- Select Emotion -- </option>';
            emotions.forEach(e => {
                const option = document.createElement("option");
                option.value = e;
                option.textContent = e;
                emotion_dropdown.appendChild(option)
            })
        })
        .catch(() => {
            emotion_dropdown.innerHTML = "<option value=''>Failed to load emotions</option>";
        })
})

function fetch_category_emotion_count() {
    const category = document.getElementById('category-dropdown').value.trim();
    const emotion = document.getElementById('emotion-dropdown').value.trim();
    const output_div = document.getElementById('task4-output');
    //clear prev results
    output_div.innerHTML = "";

    //ensure both fields are filled
    if (!category || !emotion) {
        output_div.innerText = "Please enter both a valid category and a valid emotion";
        return;
    }

    if (category == "Running") {
        const url = `/get_run_emotion_count?category=${encodeURIComponent(category)}&emotion=${encodeURIComponent(emotion)}`

        fetch(url)
            .then(res => res.json())
            .then(data => {
                const found = data.run_emotion_counts.find(
                    entry => entry[0].toLowerCase() === emotion.toLowerCase()
                )

                if (found) {
                    const count = found[1]
                    output_div.innerText = `Category: ${category}, Emotion: ${emotion}, Count: ${count}`
                } else {
                    output_div.innerText = "Combination does not exist."
                }
            })
            .catch(error => {
                console.error("Error fetching data:", error);
            });
    }
    else if (category == "Work/Study") {
        const url = `/get_study_emotion_count?category=${encodeURIComponent(category)}&emotion=${encodeURIComponent(emotion)}`

        fetch(url)
            .then(res => res.json())
            .then(data => {
                const found = data.study_emotion_counts.find(
                    entry => entry[0].toLowerCase() === emotion.toLowerCase()
                )

                if (found) {
                    const count = found[1]
                    output_div.innerText = `Category: ${category}, Emotion: ${emotion}, Count: ${count}`
                } else {
                    output_div.innerText = "Combination does not exist."
                }
            })
            .catch(error => {
                console.error("Error fetching data:", error);
            });
    }
    else if (category == "Relaxation/Meditation") {
        const url = `/get_relax_emotion_count?category=${encodeURIComponent(category)}&emotion=${encodeURIComponent(emotion)}`

        fetch(url)
            .then(res => res.json())
            .then(data => {
                const found = data.relax_emotion_counts.find(
                    entry => entry[0].toLowerCase() === emotion.toLowerCase()
                )

                if (found) {
                    const count = found[1]
                    output_div.innerText = `Category: ${category}, Emotion: ${emotion}, Count: ${count}`
                } else {
                    output_div.innerText = "Combination does not exist."
                }
            })
            .catch(error => {
                console.error("Error fetching data:", error);
            });
    }
}