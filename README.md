# Continuous Auditing Pilot

Local Django pilot for weekly continuous auditing over `SOC` and `JGS` Excel tabs.

## Run locally

```powershell
.\.venv\Scripts\python -m pip install -r requirements.txt
.\.venv\Scripts\python manage.py migrate
.\.venv\Scripts\python manage.py runserver
```

## Test

```powershell
.\.venv\Scripts\python manage.py test
```
