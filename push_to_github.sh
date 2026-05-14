#!/bin/bash

# Replace YOUR_GITHUB_TOKEN with your actual token
TOKEN="YOUR_GITHUB_TOKEN"

# Update remote URL with token
git remote set-url origin https://${TOKEN}@github.com/ekhdaisolabellatev-eng/chicago-crimes-workspace.git

# Push to GitHub
git push origin main

echo "✅ Push completed!"
