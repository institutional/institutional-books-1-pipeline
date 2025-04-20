# MACOS - Brew dependencies (partial)
if [[ "$OSTYPE" == "darwin"* ]]; then
    brew install pkg-config icu4c protobuf sqlite;
    export PATH="$(brew --prefix)/opt/icu4c/bin:$(brew --prefix)/opt/icu4c/sbin:$PATH";
    export PKG_CONFIG_PATH="$PKG_CONFIG_PATH:$(brew --prefix)/opt/icu4c/lib/pkgconfig";
fi

# DEBIAN & CO | System-level dependencies (partial)
if command -v apt >/dev/null 2>&1; then
    sudo apt install protobuf-compiler pkg-config libicu-dev sqlite3
fi

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