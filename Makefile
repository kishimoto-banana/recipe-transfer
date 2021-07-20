fmt:
	poetry run black .  

transfer:
	poetry run python main.py

master:
	poetry run python upload_master.py
