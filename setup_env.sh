#!/bin/bash
set -e

# Define environment directory (uv defaults to .venv, standard is venv)
VENV_DIR=".venv"

echo "🚀 Setting up Python environment using 'uv'..."

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "❌ 'uv' is not installed. Installing it now..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Add uv to path for current session if needed
    source "$HOME/.cargo/env" 2>/dev/null || true
fi

# Create virtual environment
if [ ! -d "$VENV_DIR" ]; then
    echo "📦 Creating virtual environment in $VENV_DIR..."
    uv venv $VENV_DIR
else
    echo "✅ Virtual environment already exists."
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source $VENV_DIR/bin/activate

# Install dependencies
if [ -f "requirements.txt" ]; then
    echo "📥 Installing dependencies from requirements.txt..."
    uv pip install -r requirements.txt
else
    echo "⚠️ requirements.txt not found!"
fi

echo "✨ Setup complete! To activate the environment, run:"
echo "source $VENV_DIR/bin/activate"
