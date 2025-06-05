document.addEventListener('DOMContentLoaded', () => {
    const dropdown = document.getElementById('genre-dropdown');
  
    fetch('/get_distinct_genres')
      .then(res => res.json())
      .then(genres => {
        dropdown.innerHTML = '<option value=""> -- Select Genre -- </option>';
        genres.forEach(genre => {
          const opt = document.createElement('option');
          opt.value = genre;
          opt.textContent = genre;
          dropdown.appendChild(opt);
        })
      })
      .catch(() => {
        dropdown.innerHTML = "<option value=''>Failed to load genres</option>";
    })
  })
  
  function fetchPeakForGenre() {
    const selected = document.getElementById('genre-dropdown').value;
    const output = document.getElementById('peak-output');
  
    if (!selected) {
      output.innerHTML = "";
      return;
    }
  
    fetch(`/get_peak_for_genre/${encodeURIComponent(selected)}`)
      .then(res => res.json())
      .then(data => {
        if (data.error) {
          output.innerHTML = `<p style="color:red">${data.error}</p>`;
        } else {
          output.innerHTML = `<strong>${selected}</strong> peaked in <strong>${data.year}</strong> with a popularity score of <strong>${data.popularity}</strong>`;
        }
      })
      .catch(err => {
        console.error(err);
        output.innerHTML = `<p style="color:red">Failed to fetch data.</p>`;
      });
  }
  
  function closeAllTrends() {
    const outputDiv = document.getElementById('all-trends-output');
    outputDiv.innerHTML = "";
  }
  
  function fetchAllTrends() {
    fetch('/get_all_trends')
      .then(res => res.json())
      .then(data => {
        const outputDiv = document.getElementById('all-trends-output');
        if (!data.length) {
          outputDiv.innerHTML = "<p>No trends found.</p>";
          return;
        }
  
        let html = `<table border="1" cellpadding="5" cellspacing="0">
          <thead>
            <tr><th>Genre</th><th>Year</th><th>Popularity</th></tr>
          </thead>
          <tbody>`;
  
        data.forEach(row => {
          html += `<tr>
            <td>${row.genre}</td>
            <td>${row.year}</td>
            <td>${row.popularity}</td>
          </tr>`;
        });
  
        html += "</tbody></table>";
        outputDiv.innerHTML = html;
      })
      .catch(err => {
        console.error(err);
        document.getElementById('all-trends-output').innerHTML = "<p style='color:red'>Failed to load trends.</p>";
      });
  }
