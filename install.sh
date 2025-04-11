# Poetry dependencies
poetry env use 3.12;
poetry install;

# .env file
cp .env.example .env;

# External dependencies and github repos
mkdir tmp;
cd tmp;
git clone https://github.com/ppaanngggg/layoutreader.git
cd ..;