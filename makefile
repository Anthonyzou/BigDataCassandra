generate:
	scp -i NebulaLaunchKey.pem pythonClient/cdr_table.sql group3@199.116.235.57:/home/group3
	scp -i NebulaLaunchKey.pem pythonClient/tablestuffs.txt group3@199.116.235.57:/home/group3
	cat pythonClient/generate.py | ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "python "
query:
	cat  pythonClient/query.py | ssh -i NebulaLaunchKey.pem group3@199.116.235.57 "python "
instance1:
	ssh -x -i NebulaLaunchKey.pem group3@199.116.235.57
