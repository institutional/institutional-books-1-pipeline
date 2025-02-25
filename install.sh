# Poetry dependencies
poetry env use 3.12;
poetry install;

# .env file
cp .env.example .env;

# Spacy models
poetry run python -m spacy download xx_sent_ud_sm;