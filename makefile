java:
	ant
	scp -i NebulaLaunchKey.pem javaClient/bin/client.jar group3@199.116.235.57:/home/group3
python:
	scp -i NebulaLaunchKey.pem javaClient/bin/client.jar group3@199.116.235.57:/home/group3

instance1:
	ssh -i NebulaLaunchKey.pem group3@199.116.235.57
