## Installing

You need at least Python 3.12 and Poetry 1.7.

```bash
poetry install # Install dependencies
poetry run pt/index.py # Run the validations and get the results pages; can take a while, ~5min
python3 -m http.server 8000 # Serve the results pages
```

and navigate to `http://localhost:8000/pt/` in your browser.
