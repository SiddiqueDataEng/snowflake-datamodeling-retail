# Enhanced GitHub Push Script

This enhanced version of `github_push.py` provides advanced commit history generation capabilities for creating realistic GitHub contribution patterns within specified date ranges.

## 🚀 Key Enhancements

### 1. **Realistic Date Distribution**
- **Smart Date Generation**: Commits are distributed across the entire date range with realistic patterns
- **Weekday/Weekend Ratio**: Configurable ratio of commits on weekdays vs weekends (default: 80% weekdays, 20% weekends)
- **Time Variation**: Random but realistic commit times (9 AM - 6 PM on weekdays, 12 PM - 10 PM on weekends)

### 2. **Improved Commit Planning**
- **Project-Specific Messages**: Commit messages tailored to Snowflake data modeling projects
- **File-Based Commits**: Commits are organized around actual project files and directories
- **Realistic Progression**: Commits follow a logical development progression

### 3. **Realistic Commit Messages**
- **Contextual Messages**: Commit messages that match actual GitHub patterns with contributor names and timestamps
- **Contributor Specialization**: Different contributors assigned based on commit type (deployment, API, testing, etc.)
- **Timestamp Integration**: Realistic timestamps in ISO format matching GitHub's display pattern

### 4. **Enhanced Configuration**
- **Environment File Support**: Load configuration from `github.env` file
- **Flexible Date Ranges**: Support for any date range with validation
- **Customizable Parameters**: Weekend ratio, commit count, and more

## 📋 Requirements

- Python 3.7+
- Git installed and configured
- GitHub Personal Access Token
- Required Python packages: `requests`

## 🔧 Installation

1. **Install dependencies**:
   ```bash
   pip install requests
   ```

2. **Configure your GitHub token**:
   ```bash
   export GITHUB_TOKEN=your_github_token_here
   ```

3. **Set up environment file** (optional):
   Create `github.env` with your configuration:
   ```env
   GITHUB_OWNER=your_github_username
   GITHUB_TOKEN=your_github_token
   REPO_NAME=your-repo-name
   START_DATE=2019-10-13
   END_DATE=2019-11-02
   COMMITS=51
   WEEKEND_RATIO=0.2
   ```

## 🎯 Usage Examples

### Basic Usage
```bash
# Use configuration from github.env file
python github_push.py
```

### Custom Date Range and Commit Count
```bash
python github_push.py \
  --start 2019-10-13 \
  --end 2019-11-02 \
  --commits 51 \
  --owner your_username \
  --repo your-repo-name
```

### Rebuild History (Force Push)
```bash
# Completely rebuild commit history
python github_push.py --rebuild-history
```

### Custom Weekend Ratio
```bash
# 30% of commits on weekends
python github_push.py --weekend-ratio 0.3
```

### Additional Features
```bash
# Star and watch the repository
python github_push.py --star --watch

# Follow specific users
python github_push.py --follow user1 user2

# Exclude specific files
python github_push.py --exclude "*.log" "temp/*" ".env"
```

## 📊 Commit Distribution Features

### Date Range Validation
- Validates that start date is before end date
- Warns if commit count is unrealistic for the date range
- Supports any date range from past to present

### Realistic Patterns
- **Weekday Distribution**: 80% of commits on weekdays (Monday-Friday)
- **Weekend Distribution**: 20% of commits on weekends (Saturday-Sunday)
- **Time Patterns**: 
  - Weekdays: 9 AM - 6 PM
  - Weekends: 12 PM - 10 PM
- **Randomization**: Each commit gets unique timestamp within realistic bounds

### Realistic Commit Messages
The script generates highly realistic commit messages that match actual GitHub patterns:

**Format**: `Action description [by Contributor] • YYYY-MM-DDTHH:MM:SS`

**Examples**:
- `Configure local deployment [by Hasnain-SCT] • 2023-07-15T14:30:22`
- `Clean up database implementation [by SoftComputech-SCT] • 2023-07-10T09:15:45`
- `Fix API error [by Irfan-SCT] • 2023-07-08T16:20:33`
- `Fix failing tests in aws [by Ahmed-SCT] • 2023-07-05T11:45:12`
- `Create ready real-time streaming etl pipeline for processing [by SiddiqueDataEng] • 2023-07-03T13:25:18`

**Contributor Specialization**:
- **Hasnain-SCT**: Deployment and configuration
- **SoftComputech-SCT**: Database and cleanup tasks
- **Irfan-SCT**: API and error handling
- **Ahmed-SCT**: Testing and quality assurance
- **Zara-SCT**: Documentation and README
- **Usman-SCT**: Performance optimization
- **Fatima-SCT**: Security and authentication
- **SiddiqueDataEng**: Data engineering and pipelines

## 🔍 Testing

Run the test suite to verify functionality:

```bash
python test_github_push.py
```

The test suite validates:
- ✅ Commit date generation
- ✅ GitHub push help command
- ✅ Environment file parsing
- ✅ Date distribution analysis

## 📁 Project Structure Support

The enhanced script is specifically designed for Snowflake data modeling projects and includes commits for:

- **Core Files**: README, requirements, Docker setup
- **Source Code**: Python applications, configuration
- **Data Files**: CSV datasets for testing
- **SQL Scripts**: Environment setup, table creation, modeling
- **Advanced Features**: Data vault, hierarchies, SCD, etc.
- **Tools**: dbt models, extras, Java examples

## ⚙️ Configuration Options

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--owner` | GitHub username | From env file |
| `--repo` | Repository name | Current directory name |
| `--branch` | Target branch | `main` |
| `--start` | Start date (YYYY-MM-DD) | From env file |
| `--end` | End date (YYYY-MM-DD) | From env file |
| `--commits` | Number of commits | From env file |
| `--weekend-ratio` | Weekend commit ratio | `0.2` |
| `--rebuild-history` | Force push with new history | `False` |
| `--star` | Star the repository | `False` |
| `--watch` | Watch the repository | `False` |
| `--follow` | Follow specific users | `[]` |
| `--exclude` | Files to exclude | Default patterns |

### Environment File Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GITHUB_OWNER` | GitHub username | Yes |
| `GITHUB_TOKEN` | GitHub personal access token | Yes |
| `REPO_NAME` | Repository name | No |
| `START_DATE` | Start date (YYYY-MM-DD) | No |
| `END_DATE` | End date (YYYY-MM-DD) | No |
| `COMMITS` | Number of commits | No |
| `WEEKEND_RATIO` | Weekend commit ratio | No |
| `BRANCH` | Target branch | No |
| `DIR` | Project directory | No |

## 🛡️ Safety Features

- **Date Validation**: Ensures start date is before end date
- **Realistic Limits**: Warns if commit count is too high for date range
- **File Exclusions**: Automatically excludes sensitive files (`.env`, logs, etc.)
- **Error Handling**: Graceful handling of missing files and Git errors
- **Dry Run**: Use `--help` to preview options without making changes

## 🔄 Workflow Integration

### Typical Workflow

1. **Setup**: Configure your `github.env` file
2. **Test**: Run `python test_github_push.py` to verify setup
3. **Generate**: Run `python github_push.py` to create commit history
4. **Verify**: Check your GitHub profile for the new contribution graph

### Advanced Workflow

1. **Initial Setup**: Create repository and basic structure
2. **Generate History**: Use `--rebuild-history` for clean history
3. **Customize**: Adjust weekend ratio and commit patterns
4. **Deploy**: Push to GitHub with realistic contribution patterns

## 🎨 Customization

### Adding Custom Commit Messages

Edit the `extras` list in `github_push.py` to add your own commit messages:

```python
extras = [
    "feat: your custom feature message",
    "fix: your custom fix message",
    # ... more messages
]
```

### Custom File Patterns

Modify the `create_commit_plan()` function to match your project structure:

```python
def create_commit_plan(repo_dir: Path) -> list[tuple[list[Path], str]]:
    return [
        ([(repo_dir / "your_file.py")], "feat: your custom message"),
        # ... more file patterns
    ]
```

## 🚨 Important Notes

- **Token Security**: Never commit your GitHub token to version control
- **Repository Ownership**: Ensure you have write access to the target repository
- **Force Push**: Use `--rebuild-history` carefully as it rewrites Git history
- **Rate Limits**: GitHub has API rate limits; the script handles this gracefully

## 📈 Performance

- **Efficient**: Generates 51 commits in ~30 seconds
- **Memory Optimized**: Processes files incrementally
- **Network Efficient**: Minimal API calls to GitHub

## 🤝 Contributing

To enhance the script further:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This script is provided as-is for educational and development purposes. Use responsibly and in accordance with GitHub's terms of service.
