from datetime import date, datetime

print(date.today())
now = datetime.now()
print(now)
print(now.date())
print(now.hour)
print(now.minute)
hours = str(now.hour) + ':' + str(now.minute)
print(hours)