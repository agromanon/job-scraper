#!/bin/bash

# GitHub Deployment Setup Script for JobScraper
# This script helps you set up GitHub deployment quickly

set -e

echo "🚀 JobScraper GitHub Deployment Setup"
echo "====================================="

# Check if git is initialized
if [ ! -d ".git" ]; then
    echo "📋 Initializing git repository..."
    git init
    git branch -m main
else
    echo "✅ Git repository already initialized"
fi

# Check GitHub remote
if ! git remote -v | grep -q "origin.*github.com"; then
    echo "❌ No GitHub remote found!"
    echo "Please create a GitHub repository and run:"
    echo "git remote add origin https://github.com/YOUR_USERNAME/job-scraper.git"
    echo "git push -u origin main"
    exit 1
else
    echo "✅ GitHub remote configured"
fi

# Add all files
echo "📦 Adding files to git..."
git add .

# Check if there are changes to commit
if git diff --cached --quiet; then
    echo "🟡 No changes to commit"
else
    echo "💾 Creating initial commit..."
    git commit -m "Initial commit: Job scraping system

- Modular worker management system
- Flask admin interface for configuration
- Support for LinkedIn, Indeed, Glassdoor, and other job sites
- EasyPanel deployment configuration
- GitHub integration with automatic deployment
- Database schema for worker and execution tracking
- Comprehensive documentation and deployment guides

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"
fi

# Initial push if needed
if git branch --show-current | grep -q "main" && ! git log --oneline origin/main..main 2>/dev/null | grep -q .; then
    echo "📤 Pushing to GitHub..."
    git push -u origin main
else
    echo "🟡 Repository already pushed or not on main branch"
fi

echo ""
echo "✅ GitHub setup complete!"
echo ""
echo "🎯 Next Steps:"
echo "1. Log into EasyPanel dashboard"
echo "2. Click 'Add Service' → 'Git Repository'"
echo "3. Repository URL: $(git remote get-url origin)"
echo "4. Branch: main"
echo "5. Dockerfile: Dockerfile.force_rebuild_ultimate (for main service)"
echo "6. Set environment variables in EasyPanel"
echo "7. Click 'Deploy'"
echo ""
echo "📚 Documentation created:"
echo "- GITHUB_DEPLOYMENT.md - Complete GitHub deployment guide"
echo "- EASYPANEL_DEPLOYMENT.md - EasyPanel deployment options"
echo "- DEPLOYMENT_CHECKLIST.md - Pre-deployment verification"
echo "- TROUBLESHOOTING_BUILDS.md - Build error solutions"
echo ""
echo "🔗 Ready for GitHub → EasyPanel deployment!"
echo ""