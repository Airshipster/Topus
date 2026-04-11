name: Check YouTube Videos

on:
  schedule:
    # ВАЖНО: GitHub Actions может задерживать выполнение!
    # Используем более частый интервал
    - cron: '*/5 * * * *'
  
  workflow_dispatch:

jobs:
  check-and-publish:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    
    steps:
    - name: Checkout repository
      uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
    
    - name: Run main script
      env:
        GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON }}
      run: |
        python src/main.py
